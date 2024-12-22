use std::collections::VecDeque;
use std::ffi::{c_int, c_void};
use std::io::{Error, Result};
use std::mem;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{ready, Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A wrapper around a `UnixStream` that allows file descriptors to be
/// sent and received with messages.  Implements `AsyncRead` and
/// `AsyncWrite` such that standard asynchronous reading and writing
/// operations and helpers may be used.
pub struct UnixFdStream<T: AsRawFd> {
    inner: AsyncFd<T>,
    outgoing_fds: Mutex<Vec<RawFd>>,
    incoming_fds: Mutex<VecDeque<RawFd>>,
    max_read_fds: usize,
}

unsafe fn close_fds<T: IntoIterator<Item = RawFd>>(fds: T) {
    for fd in fds.into_iter() {
        libc::close(fd);
    }
}

struct Header {
    layout: std::alloc::Layout,
    data_length: usize,
    header_length: usize,
    pointer: *mut u8,
}

impl Header {
    fn new(fd_count: usize) -> Result<Self> {
        let data_length = mem::size_of::<c_int>() * fd_count;
        // SAFETY: This isn't actually an unsafe operation, it's just some mem::size_of operations.
        let header_length = unsafe { libc::CMSG_SPACE(data_length as u32) as usize };
        // Create a header buffer that is large enough to store header_length, but aligned to cmsghdr.
        let align = mem::align_of::<libc::cmsghdr>();
        let size = std::cmp::max(header_length, mem::size_of::<libc::msghdr>());
        let Ok(layout) = std::alloc::Layout::from_size_align(size, align) else {
            return Err(Error::from(std::io::ErrorKind::OutOfMemory));
        };
        // SAFETY: We will look after this pointer and ensure that it is deallocated.
        let pointer = unsafe { std::alloc::alloc_zeroed(layout) };
        if pointer.is_null() {
            return Err(Error::from(std::io::ErrorKind::OutOfMemory));
        }
        Ok(Self {
            data_length,
            header_length,
            layout,
            pointer,
        })
    }

    fn as_ptr(&self) -> *mut c_void {
        self.pointer as *mut c_void
    }
}

impl Drop for Header {
    fn drop(&mut self) {
        // SAFETY: This object only exists if this was allocated by us.
        unsafe {
            std::alloc::dealloc(self.pointer, self.layout);
        }
    }
}

impl<T: AsRawFd> UnixFdStream<T> {
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
        //unix.set_nonblocking(true)?;
        Ok(Self {
            inner: AsyncFd::new(unix)?,
            outgoing_fds: Mutex::new(Vec::new()),
            incoming_fds: Mutex::new(VecDeque::new()),
            max_read_fds,
        })
    }

    /// Push a file descriptor to be written with the next message that
    /// is written to this stream.  The ownership is transferred and the
    /// file descriptor is either closed when the message is sent or this
    /// instance is dropped.
    pub fn push_outgoing_fd<F: IntoRawFd>(&self, fd: F) {
        if let Ok(mut guard) = self.outgoing_fds.lock() {
            guard.push(fd.into_raw_fd());
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

    fn write_simple(&self, buf: &[u8]) -> Result<usize> {
        // SAFETY: The socket is owned by us and the buffer is of known size.
        let rv = unsafe {
            libc::send(
                self.inner.as_raw_fd(),
                buf.as_ptr() as *const c_void,
                buf.len(),
                0,
            )
        };
        if rv < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(rv as usize)
    }

    fn add_to_outgoing(&self, mut fds: Vec<RawFd>) {
        if let Ok(mut guard) = self.outgoing_fds.lock() {
            // Inject to the front again to try and maintain ordering.
            while let Some(fd) = fds.pop() {
                guard.insert(0, fd);
            }
        } else {
            // SAFETY: We own the file descriptors, so it's safe to close them.
            unsafe {
                close_fds(fds);
            }
        }
    }

    fn raw_write(&self, buf: &[u8]) -> Result<usize> {
        let mut outgoing_fds = Vec::<RawFd>::new();
        if let Ok(mut guard) = self.outgoing_fds.lock() {
            std::mem::swap(&mut *guard, &mut outgoing_fds);
        }
        // Shortcut if there's no file descriptors to send.
        if outgoing_fds.is_empty() {
            return self.write_simple(buf);
        }
        let header = match Header::new(outgoing_fds.len()) {
            Ok(header) => header,
            Err(err) => {
                self.add_to_outgoing(outgoing_fds);
                return Err(err);
            }
        };
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
        for fd in &outgoing_fds {
            // SAFETY: We have a valid pointer to `header` and now we are copying
            //         the data that we created space for into it.
            data = unsafe {
                std::ptr::write_unaligned(data, *fd as c_int);
                data.add(1)
            };
        }
        // SAFETY: We just set up the message to send, so we're all safe to attempt to
        //         send it, also the socket that we are sending on is owned by us.
        let rv = unsafe { libc::sendmsg(self.inner.as_raw_fd(), &msg, 0) };
        if rv < 0 {
            // The operation failed, we need to put the file descriptors back.
            self.add_to_outgoing(outgoing_fds);
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: We own the file descriptors, so it's safe to close them.
        unsafe {
            close_fds(outgoing_fds);
        }
        Ok(rv as usize)
    }

    fn read_simple(&self, buf: &mut [u8]) -> Result<usize> {
        // SAFETY: The socket is owned by us and the buffer is of known size.
        let rv = unsafe {
            libc::recv(
                self.inner.as_raw_fd(),
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                0,
            )
        };
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

    fn raw_read(&self, buf: &mut [u8]) -> Result<usize> {
        // Shortcut in case this was used without any file descriptor
        // read buffer, maybe the user just wants to send file descriptors.
        if self.max_read_fds == 0 {
            return self.read_simple(buf);
        }
        let header = Header::new(self.max_read_fds)?;
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
        let read_bytes = match unsafe { libc::recvmsg(self.inner.as_raw_fd(), &mut msg, 0) } {
            0 => return Ok(0),
            rv if rv < 0 => Err(Error::last_os_error()),
            rv => Ok(rv as usize),
        }?;
        let mut read_fds = UnixFdStream::<T>::read_fds(&msg)?;
        if let Ok(mut guard) = self.incoming_fds.lock() {
            guard.append(&mut read_fds);
        } else {
            // SAFETY: We own the file descriptors, so it's safe to close them.
            unsafe {
                close_fds(read_fds);
            }
        }
        Ok(read_bytes)
    }
}

impl<T: AsRawFd> Drop for UnixFdStream<T> {
    fn drop(&mut self) {
        self.outgoing_fds.clear_poison();
        let mut fds = Vec::new();
        if let Ok(mut guard) = self.outgoing_fds.lock() {
            std::mem::swap(&mut fds, &mut *guard);
        }
        // SAFETY: It we own these file descriptors, so it's safe for us to close them.
        unsafe { close_fds(fds) };

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
            match guard.try_io(|_inner| self.raw_read(unfilled)) {
                Ok(Ok(len)) => {
                    buf.advance(len);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                Err(_would_block) => continue,
            }
        }
    }
}

impl AsyncWrite for UnixFdStream<UnixStream> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        loop {
            let mut guard = ready!(self.inner.poll_write_ready(cx))?;

            match guard.try_io(|_inner| self.raw_write(buf)) {
                Ok(result) => return Poll::Ready(result),
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
        self.inner.get_ref().shutdown(std::net::Shutdown::Write)?;
        Poll::Ready(Ok(()))
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
        let first_join = tokio::spawn(async move {
            let mut first = UnixFdStream::new(first, 0).unwrap();
            let (third, fourth) = std::os::unix::net::UnixStream::pair().unwrap();
            let mut third = tokio::net::UnixStream::from_std(third).unwrap();
            first.push_outgoing_fd(fourth);
            first.write(b"test\n").await.unwrap();
            third.write(b"test\n").await.unwrap();
        });
        let second_join = tokio::spawn(async move {
            let second = tokio::io::BufReader::new(UnixFdStream::new(second, 4).unwrap());
            let mut lines = second.lines();
            assert_eq!("test", lines.next_line().await.unwrap().unwrap());
            assert_eq!(1, lines.get_ref().get_ref().incoming_count());
            let third = unsafe {
                std::os::unix::net::UnixStream::from_raw_fd(
                    lines.get_ref().get_ref().pop_incoming_fd().unwrap(),
                )
            };
            let third = tokio::io::BufReader::new(tokio::net::UnixStream::from_std(third).unwrap());
            assert_eq!("test", third.lines().next_line().await.unwrap().unwrap());
        });
        let (first_result, second_result) = tokio::join!(first_join, second_join);
        first_result.unwrap();
        second_result.unwrap();
    }
}
