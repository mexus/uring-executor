use std::net::{Ipv6Addr, TcpListener, TcpStream};

use anyhow::Context;
use io_uring::IoUring;
use uring_executor::{ListenerExt as _, Runtime, SocketAddress, StreamExt as _};

fn main() -> anyhow::Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "INFO");
    }
    env_logger::init();

    let ring = IoUring::new(100).context("IoUring::new")?;
    let mut runtime = Runtime::new(ring);

    let listener =
        TcpListener::bind((Ipv6Addr::UNSPECIFIED, 1024)).context("Listen to port [::]:1024")?;

    let listening_address = listener.local_addr().context("TcpListened::local_addr")?;
    log::info!("Listening at {}", listening_address);

    if let Err(e) = runtime.block_on(run(&listener, vec![0u8; 1024], SocketAddress::new())) {
        log::error!("Accepting loop terminated with error: {:#}", e)
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
        log::info!("Waiting for connection");
        let (stream, peer_address) = match listener.async_accept(address_buffer).into_future().await
        {
            Ok((stream, address)) => {
                let peer_address = address.as_socket_addr();
                address_buffer = address.into_uninit();
                (stream, peer_address)
            }
            Err((e, _address)) => {
                return Err(e).context("Accept");
            }
        };
        log::info!("Accepted connection from {}", peer_address);

        let (result, temp_buffer) = copy_all(stream, buffer).await;
        if let Err(e) = result {
            log::error!("Connection handling terminated with error: {:#}", e)
        }
        buffer = temp_buffer;
    }
}

async fn copy_all(stream: TcpStream, mut tmp_buffer: Vec<u8>) -> (anyhow::Result<()>, Vec<u8>) {
    loop {
        let (result, buffer) = stream.async_read(tmp_buffer, ..).into_future().await;
        tmp_buffer = buffer;
        let bytes_read = match result.context("Reading") {
            Ok(val) => val,
            Err(e) => return (Err(e), tmp_buffer),
        };
        if bytes_read == 0 {
            log::info!("Connection terminated");
            break;
        }
        log::debug!(
            "Received {} bytes: {:?}",
            bytes_read,
            String::from_utf8_lossy(&tmp_buffer[..bytes_read])
        );
        let (result, buffer) = stream
            .async_write(tmp_buffer, ..bytes_read)
            .into_future()
            .await;
        tmp_buffer = buffer;
        match result.context("Writing") {
            Ok(bytes) => {
                log::debug!("Sent {} bytes", bytes)
            }
            Err(e) => return (Err(e), tmp_buffer),
        }
    }
    (Ok(()), tmp_buffer)
}
