use crate::sinks::bkgse::connection::GseConnection;
use crate::sinks::bkgse::proto::GseDynamicMsg;

pub struct GseClient {
    con: GseConnection
}

impl GseClient {
    pub fn new(endpoint: String) -> Self {
        let con = GseConnection::new(endpoint);
        GseClient {
            con
        }
    }
    pub fn check(&mut self) {
        self.con.dial()
    }

    #[allow(dead_code)]
    pub fn send_event(&mut self, data: Vec<u8>, data_id: u32) {
        let msg = GseDynamicMsg::new_with_data(data, data_id);
        self.con.write(msg.to_bytes().as_slice());
    }
}