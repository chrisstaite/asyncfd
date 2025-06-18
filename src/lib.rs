use std::collections::VecDeque;
use std::ffi::{c_int, c_void};
use std::io::{Error, Result};
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

mod header;
pub mod split;
pub mod split_owned;

/// A wrapper around a `UnixStream` that allows file descriptors to be
/// sent and received with messages.  Implements `AsyncRead` and
/// `AsyncWrite` such that standard asynchronous reading and writing
/// operations and helpers may be used.
pub struct UnixFdStream<T: AsRawFd> {
    inner: AsyncFd<T>,
    incoming_fds: Mutex<VecDeque<RawFd>>,
    outgoing_tx: UnboundedSender<RawFd>,
    outgoing_rx: Option<UnboundedReceiver<RawFd>>,
    max_read_fds: usize,
}

/// This is the trait required to implement AsyncWrite for a type.
pub trait Shutdown {
    fn shutdown(&self, how: std::net::Shutdown) -> Result<()>;
}

impl Shutdown for UnixStream {
    fn shutdown(&self, how: std::net::Shutdown) -> Result<()> {
        UnixStream::shutdown(self, how)
    }
}

/// This is the trait required to create a UnixFdStream as it needs to
/// be non-blocking before it can be used.
pub trait NonBlocking {
    fn set_nonblocking(&self, nonblocking: bool) -> Result<()>;
}

impl NonBlocking for UnixStream {
    fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        UnixStream::set_nonblocking(&self, nonblocking)
    }
}

pub(crate) unsafe fn close_fds<T: IntoIterator<Item = RawFd>>(fds: T) {
    for fd in fds.into_iter() {
        libc::close(fd);
    }
}

impl<T: AsRawFd + NonBlocking> UnixFdStream<T> {
    /// Create a new `UnixFdStream` from a `UnixStream` which is also
    /// configured to read up to `max_read_fds` for each read from the
    /// socket.
    ///
    /// The file descriptors that are transferred are buffered in a
    /// `Vec<RawFd>`, but only so many will have space made for them
    /// in the receiving header as configured by `max_read_fds`, other
    /// file descriptors sent beyond this limit will be discarded by the
    /// kernel.  We do not check for the MSG_CTRUNC flag, therefore this
    /// will be a silent discard.
    pub fn new(unix: T, max_read_fds: usize) -> Result<Self> {
        unix.set_nonblocking(true)?;
        let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            inner: AsyncFd::new(unix)?,
            incoming_fds: Mutex::new(VecDeque::new()),
            outgoing_tx,
            outgoing_rx: Some(outgoing_rx),
            max_read_fds,
        })
    }
}

impl<T: AsRawFd> UnixFdStream<T> {
    pub fn split<'a>(
        &'a mut self,
    ) -> (
        crate::split::ReadHalf<'a, T>,
        crate::split::WriteHalf<'a, T>,
    ) {
        let read =
            crate::split::ReadHalf::<T>::new(&self.inner, &self.incoming_fds, &self.max_read_fds);
        let write = crate::split::WriteHalf::<T>::new(
            &self.inner,
            &self.outgoing_tx,
            self.outgoing_rx.as_mut().unwrap(),
        );
        (read, write)
    }

    pub fn into_split(
        mut self,
    ) -> (
        crate::split_owned::OwnedReadHalf<T>,
        crate::split_owned::OwnedWriteHalf<T>,
    ) {
        let rx: UnboundedReceiver<i32> = self.outgoing_rx.take().unwrap();
        let own_self = Arc::new(self);
        let write = crate::split_owned::OwnedWriteHalf::new(
            own_self.clone(),
            own_self.outgoing_tx.clone(),
            rx,
        );
        (crate::split_owned::OwnedReadHalf::new(own_self), write)
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

    /// Wait for the underlying UnixStream to become readable.
    pub async fn readable(&self) -> Result<()> {
        self.inner.readable().await?.retain_ready();
        Ok(())
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

    fn write_simple(socket: RawFd, buf: &[u8]) -> Result<usize> {
        // SAFETY: The socket is owned by us and the buffer is of known size.
        let rv = unsafe { libc::send(socket, buf.as_ptr() as *const c_void, buf.len(), 0) };
        if rv < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(rv as usize)
    }

    fn add_to_outgoing(&mut self, mut fds: Vec<RawFd>) {
        // Just in case there were other file descriptors added, pull them from the channel.
        while let Ok(fd) = self.outgoing_rx.as_mut().unwrap().try_recv() {
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

    fn raw_write(socket: RawFd, outgoing_fds: &[RawFd], buf: &[u8]) -> Result<usize> {
        if outgoing_fds.is_empty() {
            return Self::write_simple(socket, buf);
        }
        let header = crate::header::Header::new(outgoing_fds.len())?;
        let mut iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut c_void,
            iov_len: buf.len(),
        };
        // SAFETY: Not really sure why this method is unsafe.
        let control_length = unsafe { libc::CMSG_LEN(header.data_length as u32) } as _;
        let msg = libc::msghdr {
            msg_iov: &mut iov,
            msg_iovlen: 1,
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_control: header.as_ptr(),
            msg_controllen: control_length,
            msg_flags: 0,
        };
        // SAFETY: We have constructed the msghdr correctly, so this will point to
        //         the allocated memory within `header`.
        let cmsg = unsafe { &mut *libc::CMSG_FIRSTHDR(&msg) };
        cmsg.cmsg_len = control_length;
        cmsg.cmsg_type = libc::SCM_RIGHTS;
        cmsg.cmsg_level = libc::SOL_SOCKET;
        // SAFETY: We have allocated correctly aligned memory, so this will point to
        //         the allocated memory within `header`.
        let mut data = unsafe { libc::CMSG_DATA(cmsg) as *mut c_int };
        for fd in outgoing_fds {
            // SAFETY: We have a valid pointer to `header` and now we are copying
            //         the data that we created space for into it.
            data = unsafe {
                std::ptr::write_unaligned(data, *fd as c_int);
                data.add(1)
            };
        }
        // SAFETY: We just set up the message to send, so we're all safe to attempt to
        //         send it, also the socket that we are sending on is owned by us.
        let rv = unsafe { libc::sendmsg(socket, &msg, 0) };
        if rv < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(rv as usize)
    }

    fn read_simple(fd: RawFd, buf: &mut [u8]) -> Result<usize> {
        // SAFETY: The socket is owned by us and the buffer is of known size.
        let rv = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut c_void, buf.len(), 0) };
        if rv < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(rv as usize)
    }

    fn read_fds(msg: &libc::msghdr) -> Result<VecDeque<RawFd>> {
        // SAFETY: We set up the buffers correctly and we assume the kernel
        //         passes us safe data.
        let mut cmsg_ptr = unsafe { libc::CMSG_FIRSTHDR(msg) };
        let mut read_fds = VecDeque::<RawFd>::new();
        while !cmsg_ptr.is_null() {
            // SAFETY: We just checked for NULL, the header was initialised to zero
            //         and we assume the kernel passes us safe data.
            let cmsg = unsafe { &*cmsg_ptr };
            if cmsg.cmsg_level == libc::SOL_SOCKET && cmsg.cmsg_type == libc::SCM_RIGHTS {
                // SAFETY: We just checked the header type and assume that the kernel
                //         is passing us valid data.
                let mut data = unsafe { libc::CMSG_DATA(cmsg) as *const c_int };
                // SAFETY: Calculating a past the end pointer that is only accessed in
                //         an unaligned safe manner.
                let data_end =
                    unsafe { (cmsg_ptr as *const u8).add(cmsg.cmsg_len as usize) as *const i32 };
                while data < data_end {
                    // SAFETY: We are checking that the data is within the header size
                    //         each iteration.
                    let fd = unsafe { std::ptr::read_unaligned(data) };
                    // SAFETY: The kernel just passed us this file descriptor.
                    let result = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
                    read_fds.push_back(fd);
                    if result < 0 {
                        // SAFETY: We have just read these FDs, so it's safe to close them.
                        unsafe { close_fds(read_fds) };
                        return Err(Error::last_os_error());
                    }
                    // SAFETY: We are just about to test this against the past-the-end pointer
                    //         as we go around the loop.
                    data = unsafe { data.add(1) };
                }
            }
            // SAFETY: We set up the buffers correctly and we assume the kernel
            //         passes us safe data.
            cmsg_ptr = unsafe { libc::CMSG_NXTHDR(msg, cmsg_ptr) };
        }
        Ok(read_fds)
    }

    fn raw_read(
        max_read_fds: usize,
        fd: RawFd,
        buf: &mut [u8],
    ) -> Result<(usize, VecDeque<RawFd>)> {
        // Shortcut in case this was used without any file descriptor
        // read buffer, maybe the user just wants to send file descriptors.
        if max_read_fds == 0 {
            return Self::read_simple(fd, buf).map(|bytes| (bytes, VecDeque::new()));
        }
        let header = crate::header::Header::new(max_read_fds)?;
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut c_void,
            iov_len: buf.len(),
        };
        // SAFETY: Just calculating the length of the header to send.
        let control_length = unsafe { libc::CMSG_LEN(header.header_length as u32) } as _;
        let mut msg = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut iov,
            msg_iovlen: 1,
            msg_control: header.as_ptr(),
            msg_controllen: control_length,
            msg_flags: 0,
        };
        // SAFETY: We own the socket and have just created and set up the message
        //         headers correctly.
        let read_bytes = match unsafe { libc::recvmsg(fd, &mut msg, 0) } {
            0 => return Ok((0, VecDeque::new())),
            rv if rv < 0 => Err(Error::last_os_error()),
            rv => Ok(rv as usize),
        }?;
        let read_fds = UnixFdStream::<T>::read_fds(&msg)?;
        Ok((read_bytes, read_fds))
    }
}

impl<T: AsRawFd> Drop for UnixFdStream<T> {
    fn drop(&mut self) {
        if let Some(outgoing_rx) = &mut self.outgoing_rx {
            while let Ok(fd) = outgoing_rx.try_recv() {
                // SAFETY: It we own these file descriptors, so it's safe for us to close them.
                unsafe {
                    libc::close(fd);
                };
            }
        }

        self.incoming_fds.clear_poison();
        let mut fds = VecDeque::new();
        if let Ok(mut guard) = self.incoming_fds.lock() {
            std::mem::swap(&mut fds, &mut *guard);
        }
        // SAFETY: It we own these file descriptors, so it's safe for us to close them.
        unsafe { close_fds(fds) };
    }
}

impl<T: AsRawFd> AsyncRead for UnixFdStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            let unfilled = buf.initialize_unfilled();
            match guard
                .try_io(|inner| Self::raw_read(self.max_read_fds, inner.as_raw_fd(), unfilled))
            {
                Ok(Ok((len, mut read_fds))) => {
                    if let Ok(mut guard) = self.incoming_fds.lock() {
                        guard.append(&mut read_fds);
                    } else {
                        // SAFETY: We own the file descriptors, so it's safe to close them.
                        unsafe {
                            close_fds(read_fds);
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

impl<T: AsRawFd + Shutdown + Unpin> AsyncWrite for UnixFdStream<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let mut outgoing_fds = Vec::<RawFd>::new();
        loop {
            while let Ok(fd) = self.outgoing_rx.as_mut().unwrap().try_recv() {
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
                UnixFdStream::<UnixStream>::raw_write(inner.as_raw_fd(), &outgoing_fds, buf)
            }) {
                Ok(Ok(bytes)) => {
                    // SAFETY: We own the file descriptors, so it's safe to close them.
                    unsafe {
                        close_fds(outgoing_fds);
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
        Poll::Ready(Shutdown::shutdown(
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
    async fn send_fd() {
        let (first, second) = std::os::unix::net::UnixStream::pair().unwrap();
        let sender = tokio::spawn(async move {
            let mut first = UnixFdStream::new(first, 0).unwrap();
            let (third, fourth) = std::os::unix::net::UnixStream::pair().unwrap();
            let mut third = tokio::net::UnixStream::from_std(third).unwrap();
            first.push_outgoing_fd(fourth);
            first.write_all(b"test\n").await.unwrap();
            first.shutdown().await.unwrap();
            third.write_all(b"test\n").await.unwrap();
            third.shutdown().await.unwrap();
            // If we drop third before receiver has finished reading then the test is not
            // stable, therefore we keep alive until the receiver drops its end.
            let _ = third.readable().await;
        });
        let receiver = tokio::spawn(async move {
            let second = tokio::io::BufReader::new(UnixFdStream::new(second, 4).unwrap());
            let mut lines = second.lines();
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
