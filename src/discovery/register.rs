use std::net::Ipv4Addr;

use mdns_sd::ServiceDaemon;
use mdns_sd::ServiceInfo;

use crate::discovery::join_delim;
use crate::discovery::SERVICE_TYPE;
use crate::discovery::join;
use crate::discovery::underscore;
use crate::error::Res;

/// Advertises the service over MDNS
pub struct Advertiser {

}

pub fn register(application: &'static str, ipv4: Ipv4Addr, port: u16) -> Res<()> {
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

    let mdns = ServiceDaemon::new()?;
    let service_info = ServiceInfo::new(
        &service_type,
        &instance_name,
        &host_name,
        &ip_string,
        port,
        None
    );
    Ok(())
}
