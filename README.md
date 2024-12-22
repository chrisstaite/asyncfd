# Asyncfd

[![crates.io](https://img.shields.io/crates/v/asyncfd.svg)](https://crates.io/crates/asyncfd)
[![github.com](https://github.com/chrisstaite/asyncfd/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/chrisstaite/asyncfd/actions/workflows/ci.yml)

This crate wraps the complexities of sending and receiving file descriptors
over Unix domain sockets while performing asynchronous reading and writing
on the socket at the same time.

## Usage

See the `examples` directory.

### Add Rust Crate Template as a dependency

```toml
# Cargo.toml

[dependencies]
asyncfd = "0.1.0"
```

## Notable gotchas

There's no type information sent, so converting the `RawFd` back to a typed file descriptor is
an inherently unsafe operation.

Message headers can become concatenated within the kernel.  It is highly recommended to have a
handshake between the two communicating processes to ensure that the file descriptors do not get
mixed up.  This crate does its best to maintain file descriptor send/receive ordering.

## Similar crates

[passfd](https://github.com/polachok/passfd)
[fd-passing](https://crates.io/crates/fd-passing)

Both of these crates use dummy data to pass file descriptors, whereas this crate performs the file descriptor passing as a side channel to the standard read/write of the socket.  With the wrapper
provided in this crate, the socket can simply be used by Tokio in a normal way but the implementor
can add and take file descriptors depending on the sent message.

## License

This project is licensed with the
[Apache v2](https://github.com/chrisstaite/asyncfd/blob/main/LICENSE).
