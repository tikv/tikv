// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! An example TiKV storage engine.
//!
//! This project is intended to serve as a skeleton for other engine
//! implementations. It lays out the complex system of engine modules and traits
//! in a way that is consistent with other engines. To create a new engine
//! simply copy the entire directory structure and replace all "Panic*" names
//! with your engine's own name; then fill in the implementations.
