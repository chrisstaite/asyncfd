use std::ffi::{c_int, c_void};
use std::io::{Error, Result};
use std::mem;

pub(crate) struct Header {
    layout: std::alloc::Layout,
    pub(crate) data_length: usize,
    pub(crate) header_length: usize,
    pointer: *mut u8,
}

impl Header {
    pub(crate) fn new(fd_count: usize) -> Result<Self> {
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

    pub(crate) fn as_ptr(&self) -> *mut c_void {
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
