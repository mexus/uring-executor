use std::net::{Ipv6Addr, TcpListener, TcpStream};

use anyhow::Context;
use io_uring::IoUring;
use uring_executor::{ListenerExt, Runtime, SocketAddress, StreamExt};

fn main() -> anyhow::Result<()> {
    let ring = IoUring::new(100).context("IoUring::new")?;
    let mut runtime = Runtime::new(ring);

    let listener =
        TcpListener::bind((Ipv6Addr::UNSPECIFIED, 1024)).context("Listen to port [::]:1024")?;

    if let Err(e) = runtime.block_on(run(&listener, vec![0u8; 1024], SocketAddress::new())) {
        eprintln!("Accepting loop terminated with error: {:#}", e)
    }

    Ok(())
}

enum Never {}

async fn run(
    listener: &TcpListener,
    mut buffer: Vec<u8>,
    mut address_buffer: SocketAddress<uring_executor::address::Uninitialized>,
) -> anyhow::Result<Never> {
    loop {
        println!("Waiting for connection");
        let (result, temp_address_buffer) = listener.async_accept(address_buffer).await;
        let peer_address = temp_address_buffer.as_socket_addr();
        address_buffer = temp_address_buffer.into_uninit();

        let stream = result.context("Accept")?;
        println!("Accepted connection from {}", peer_address);

        let (result, temp_buffer) = copy_all(stream, buffer).await;
        if let Err(e) = result {
            eprintln!("Connection handling terminated with error: {:#}", e)
        }
        buffer = temp_buffer;
    }
}

async fn copy_all(stream: TcpStream, mut tmp_buffer: Vec<u8>) -> (anyhow::Result<()>, Vec<u8>) {
    loop {
        let (result, buffer) = stream.async_read(tmp_buffer, ..).await;
        tmp_buffer = buffer;
        let bytes_read = match result.context("Reading") {
            Ok(val) => val,
            Err(e) => return (Err(e), tmp_buffer),
        };
        if bytes_read == 0 {
            println!("Connection terminated");
            break;
        }
        // println!(
        //     "Received {} bytes: {:?}",
        //     bytes_read,
        //     String::from_utf8_lossy(&tmp_buffer[..bytes_read])
        // );
        let (result, buffer) = stream.async_write(tmp_buffer, ..bytes_read).await;
        tmp_buffer = buffer;
        match result.context("Writing") {
            Ok(_bytes) => {} // println!("Sent {} bytes", bytes),
            Err(e) => return (Err(e), tmp_buffer),
        }
    }
    (Ok(()), tmp_buffer)
}
