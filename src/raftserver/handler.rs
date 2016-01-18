#![allow(dead_code)]
#![allow(unused_variables)]

use std::vec::Vec;

use mio::{EventLoop, Token};

use raftserver::{Result, ConnData};
use raftserver::server::Server;

pub trait ServerHandler :Sized {
    // Handle messages reading from connection with Token.
    // Return some messages for later writing to this connection.
    fn handle_read_data(&mut self,
                        event_loop: &mut EventLoop<Server<Self>>,
                        token: Token,
                        msgs: Vec<ConnData>)
                        -> Result<(Vec<ConnData>)> {
        Ok((msgs))
    }

    fn handle_tick(&mut self, event_loop: &mut EventLoop<Server<Self>>) -> Result<()> {
        Ok(())
    }
}
