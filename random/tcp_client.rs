use std::io::{Write};
use std::net::{TcpStream};


fn main() {
    const PORT: u32 = 1337;
    let server = format!("127.0.0.1:{PORT}");
    let mut conn = TcpStream::connect(server).unwrap();

    conn.write("Hello, world".as_bytes()).unwrap();
}
