use std::collections::VecDeque;
use std::io::Result;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{ready, Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct ReadHalf<'a, T: AsRawFd> {
    inner: &'a AsyncFd<T>,
    incoming_fds: &'a Mutex<VecDeque<RawFd>>,
    max_read_fds: &'a usize,
}

pub struct WriteHalf<'a, T: AsRawFd> {
    inner: &'a AsyncFd<T>,
    outgoing_tx: &'a UnboundedSender<RawFd>,
    outgoing_rx: &'a mut UnboundedReceiver<RawFd>,
}

impl<'a, T: AsRawFd> WriteHalf<'a, T> {
    pub(crate) fn new(
        inner: &'a AsyncFd<T>,
        outgoing_tx: &'a UnboundedSender<RawFd>,
        outgoing_rx: &'a mut UnboundedReceiver<RawFd>,
    ) -> Self {
        Self {
            inner,
            outgoing_tx,
            outgoing_rx,
        }
    }

    /// Push a file descriptor to be written with the next message that
    /// is written to this stream.  The ownership is transferred and the
    /// file descriptor is either closed when the message is sent or this
    /// instance is dropped.
    pub fn push_outgoing_fd<F: IntoRawFd>(&self, fd: F) {
        if let Err(fd) = self.outgoing_tx.send(fd.into_raw_fd()) {
            // This should never happen, but implemented for completeness.
            // SAFETY: We just failed to push this file descriptor, so we have to
            //         close it.
            unsafe {
                libc::close(fd.0);
            }
        }
    }

    fn add_to_outgoing(&mut self, mut fds: Vec<RawFd>) {
        // Just in case there were other file descriptors added, pull them from the channel.
        while let Ok(fd) = self.outgoing_rx.try_recv() {
            fds.push(fd);
        }
        // Push all the file descriptors to the channel in order.
        for fd in fds.into_iter() {
            if let Err(fd) = self.outgoing_tx.send(fd) {
                // This is impossible as we own the rx, but just for completeness.
                // SAFETY: We own this file descriptor and are about to drop it on the
                //         floor, so it's safe to close it.
                unsafe {
                    libc::close(fd.0);
                }
            }
        }
    }
}

impl<'a, T: AsRawFd> ReadHalf<'a, T> {
    pub(crate) fn new(
        inner: &'a AsyncFd<T>,
        incoming_fds: &'a Mutex<VecDeque<RawFd>>,
        max_read_fds: &'a usize,
    ) -> Self {
        Self {
            inner,
            incoming_fds,
            max_read_fds,
        }
    }

    /// Get the most recent file descriptor that was read with a message.
    pub fn pop_incoming_fd(&self) -> Option<RawFd> {
        if let Ok(mut guard) = self.incoming_fds.lock() {
            guard.pop_front()
        } else {
            None
        }
    }

    /// Get the number of file descriptors in the incoming queue.
    pub fn incoming_count(&self) -> usize {
        self.incoming_fds
            .lock()
            .map(|guard| guard.len())
            .unwrap_or(0)
    }
}

impl<'a, T: AsRawFd> AsyncRead for ReadHalf<'a, T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            let unfilled = buf.initialize_unfilled();
            match guard.try_io(|inner| {
                crate::UnixFdStream::<T>::raw_read(*self.max_read_fds, inner.as_raw_fd(), unfilled)
            }) {
                Ok(Ok((len, mut read_fds))) => {
                    if let Ok(mut guard) = self.incoming_fds.lock() {
                        guard.append(&mut read_fds);
                    } else {
                        // SAFETY: We own the file descriptors, so it's safe to close them.
                        unsafe {
                            crate::close_fds(read_fds);
                        }
                    }
                    buf.advance(len);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(_would_block) => continue,
            }
        }
    }
}

impl<'a, T: AsRawFd + crate::Shutdown + Unpin> AsyncWrite for WriteHalf<'a, T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let mut outgoing_fds = Vec::<RawFd>::new();
        loop {
            while let Ok(fd) = self.outgoing_rx.try_recv() {
                outgoing_fds.push(fd);
            }
            let mut guard = match self.inner.poll_write_ready(cx) {
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(err)) => {
                    self.add_to_outgoing(outgoing_fds);
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => {
                    self.add_to_outgoing(outgoing_fds);
                    return Poll::Pending;
                }
            };
            match guard.try_io(|inner| {
                crate::UnixFdStream::<T>::raw_write(inner.as_raw_fd(), &outgoing_fds, buf)
            }) {
                Ok(Ok(bytes)) => {
                    // SAFETY: We own the file descriptors, so it's safe to close them.
                    unsafe {
                        crate::close_fds(outgoing_fds);
                    }
                    return Poll::Ready(Ok(bytes));
                }
                Ok(Err(err)) => {
                    self.add_to_outgoing(outgoing_fds);
                    return Poll::Ready(Err(err));
                }
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(crate::Shutdown::shutdown(
            self.inner.get_ref(),
            std::net::Shutdown::Write,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::FromRawFd;

    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncWriteExt;

    use crate::UnixFdStream;

    #[tokio::test]
    async fn send_fd_split() {
        let (first, second) = std::os::unix::net::UnixStream::pair().unwrap();
        let sender = tokio::spawn(async move {
            let mut first = UnixFdStream::new(first, 0).unwrap();
            let (_first_read, mut first_write) = first.split();
            let (third, fourth) = std::os::unix::net::UnixStream::pair().unwrap();
            let mut third = tokio::net::UnixStream::from_std(third).unwrap();
            first_write.push_outgoing_fd(fourth);
            first_write.write_all(b"test\n").await.unwrap();
            first_write.shutdown().await.unwrap();
            third.write_all(b"test\n").await.unwrap();
            third.shutdown().await.unwrap();
            // If we drop third before receiver has finished reading then the test is not
            // stable, therefore we keep alive until the receiver drops its end.
            let _ = third.readable().await;
        });
        let receiver = tokio::spawn(async move {
            let mut second = UnixFdStream::new(second, 4).unwrap();
            let (second_read, _second_write) = second.split();
            let second_read = tokio::io::BufReader::new(second_read);
            let mut lines = second_read.lines();
            assert_eq!(Some("test"), lines.next_line().await.unwrap().as_deref());
            assert_eq!(1, lines.get_ref().get_ref().incoming_count());
            let fourth: std::os::unix::net::UnixStream = unsafe {
                std::os::unix::net::UnixStream::from_raw_fd(
                    lines.get_ref().get_ref().pop_incoming_fd().unwrap(),
                )
            };
            let fourth =
                tokio::io::BufReader::new(tokio::net::UnixStream::from_std(fourth).unwrap());
            assert_eq!(
                Some("test"),
                fourth.lines().next_line().await.unwrap().as_deref()
            );
        });
        let (send_result, receive_result) = tokio::join!(sender, receiver);
        send_result.unwrap();
        receive_result.unwrap();
    }
}
