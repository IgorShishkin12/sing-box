//! Reticulum bridge library for sing-box.
//!
//! This crate provides the Rust side of the CGO FFI layer that implements
//! the reticulum protocol. It exposes a C-compatible API for Go to call.

pub mod c_api;
pub mod config;
pub mod connection;
pub mod listener;
pub mod runtime;
