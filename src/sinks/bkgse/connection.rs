use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::net::Shutdown;

pub struct GseConnection {
    con: UnixStream,

    endpoint: String,
}

impl GseConnection {
    #[allow(dead_code)]
    pub fn new(endpoint: String) -> Self {
        let stream = match UnixStream::connect(endpoint.clone()) {
            Ok(stream) => stream,
            Err(e) => {
                panic!("connect gse failed, error({:?})", e);
            }
        };

        GseConnection {
            con: stream,
            endpoint: endpoint.clone(),
        }
    }

    /// dial connect, test gse agent is alive.
    #[allow(dead_code)]
    pub fn dial(&mut self) {
        match UnixStream::connect(self.endpoint.clone()) {
            Ok(stream) => {
                self.con = stream;
            },
            Err(e) => {
                println!("dial gse failed, error({:?})", e);
            }
        };
    }

    #[allow(dead_code)]
    pub fn close(&self) {
        match self.con.shutdown(Shutdown::Both) {
            Ok(_) => (),
            Err(e) => {
                println!("close connect failed, error({:?})", e);
            }
        }
    }

    #[allow(dead_code)]
    pub fn write(&mut self, bytes: &[u8]) {
        match self.con.write(bytes) {
            Ok(n) => {
                println!("success send size {}", n);
            },
            Err(e) => {
                println!("failed send data, {:?}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub fn read(&mut self, bytes: &mut [u8]) {
        match self.con.read(bytes) {
            Ok(n) => {
                println!("success recv size {}", n);
            },
            Err(e) => {
                println!("failed recv data, {:?}", e);
            }
        }
    }
}