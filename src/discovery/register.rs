use std::time::UNIX_EPOCH;

use mdns_sd::ServiceDaemon;
use mdns_sd::ServiceInfo;
use tokio::sync::Notify;

use crate::discovery::join_delim;
use crate::discovery::SERVICE_TYPE;
use crate::discovery::join;
use crate::discovery::underscore;
use crate::error::Res;

use tokio::sync::OnceCell;

/// MDNS advertiser
pub struct Advertiser {
    daemon: ServiceDaemon,
    instantiation_timestamp: u64
}

// Required for errors
impl std::fmt::Debug for Advertiser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Advertiser")
    }
}

pub static ADVERTISER: OnceCell<Advertiser> = OnceCell::const_new();
pub static ADVERTISER_READY: Notify = Notify::const_new();

/// Advertises the service over MDNS
pub async fn register(application: &'static str, port: u16) -> Res<()> {

    // first retrieve suitable local ipv4 address
    let ipv4 = tokio::task::spawn_blocking(local_ip_address::local_ip).await??;

    let ip_string = ipv4.to_string();
    let ip_port_string = join_delim([&ip_string, &port.to_string()], ":").replace(".", "-");

    // The service identifier unique to the application but shared by all instances
    // e.g. _application-name._tcp.local.
    let service_type = join([underscore(application), SERVICE_TYPE.to_string()]);

    // The instance identifier, unique to this instance and application
    // e.g. A-B-C-D:EFGH-application-name
    let instance_name = join_delim([ip_port_string, application.to_string()], "-");

    // The host_name under which to register the mDNS
    let host_name = join([&ip_string, ".local."]);

    // Create MDNS manager and spawn into OnceCell
    let mdns = ServiceDaemon::new()?;
    let advertiser = Advertiser {
        daemon: mdns,
        instantiation_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs()
    };

    let service_info = ServiceInfo::new(
        &service_type,
        &instance_name,
        &host_name,
        &ip_string,
        port,
        None
    )?;

    // Register this application's service (nonblocking)
    advertiser.daemon.register(service_info)?;
    ADVERTISER.set(advertiser)?;
    ADVERTISER_READY.notify_waiters();

    Ok(())
}

/// Yield reference to advertiser, or wait for it to be available
pub async fn get_advertiser<'a>() -> &'a Advertiser {
    loop {
        if let Some(advertiser) = ADVERTISER.get() {
            return advertiser;
        }

        let notified = ADVERTISER_READY.notified();

        if let Some(advertiser) = ADVERTISER.get() {
            return advertiser;
        }

        notified.await;
    }
}
