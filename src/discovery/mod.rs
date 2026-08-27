pub mod register;
pub mod discover;

pub const SERVICE_TYPE: &str = "_tcp.local.";

pub fn underscore(string: impl std::fmt::Display) -> String {
    format!("_{string}")
}

pub fn join<const N: usize>(strings: [impl ToString; N]) -> String {
    strings.into_iter().map(|x| x.to_string()).collect::<Vec<String>>().join(".")
}

pub fn join_delim<const N: usize>(strings: [impl ToString; N], delim: &str) -> String {
    strings.into_iter().map(|x| x.to_string()).collect::<Vec<String>>().join(delim)
}
