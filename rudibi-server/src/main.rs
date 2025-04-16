mod engine;

use std::io::{self, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    const PORT: u32 = 1337;
    let listener = TcpListener::bind(format!("127.0.0.1:{PORT}")).unwrap();

    for stream in listener.incoming() {
        if let Ok(mut conn) = stream {
            handle_connection(&mut conn);
        }
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(l) => println!("{l}"),
            Err(e) => panic!("{e:?}")
        }
    }
}

fn handle_connection(conn: &mut TcpStream) {
    let mut buf = Vec::new();
    conn.read_to_end(&mut buf).unwrap();
    io::stdout().write_all(&buf).unwrap();
}
