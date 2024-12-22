use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream;

use asyncfd::UnixFdStream;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;

async fn send_fd(socket: UnixStream) {
    let mut socket = UnixFdStream::new(socket, 0).expect("Unable to create AsyncFd");
    let (first, second) = UnixStream::pair().expect("Unable to create new socket");
    socket.push_outgoing_fd(first);
    let mut second = UnixFdStream::new(second, 0).expect("Unable to create AsyncFd");
    socket.write(b"initial socket\n").await.unwrap();
    second.write(b"passed socket\n").await.unwrap();
}

async fn recv_fd(socket: UnixStream) {
    let socket = tokio::io::BufReader::new(UnixFdStream::new(socket, 4).unwrap());
    let mut lines = socket.lines();
    while let Some(line) = lines.next_line().await.expect("Socket error reading line") {
        println!("{line}");
        if let Some(fd) = lines.get_ref().get_ref().pop_incoming_fd() {
            let received_socket = unsafe { UnixStream::from_raw_fd(fd) };
            let reader = tokio::io::BufReader::new(tokio::net::UnixStream::from_std(received_socket).expect("Unable to attach received socket"));
            if let Some(line) = reader.lines().next_line().await.expect("Socket error reading line on received socket") {
                println!("{line}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (first, second) = UnixStream::pair().expect("Unable to create Unix socket pair");
    let _ = tokio::join!(tokio::spawn(send_fd(first)), tokio::spawn(recv_fd(second)));
}
