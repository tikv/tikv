#![feature(test)]

extern crate test;

use test::*;
use batch_system::*;
use tikv_util::mpsc;

pub type Message = ();

pub struct Runner {
    recv: mpsc::Receiver<Message>,
}

impl Fsm for Runner {
    type Message = Message;
}

pub fn new_runner(cap: usize) -> (mpsc::LooseBoundedSender<Message>, Box<Runner>) {
    let (tx, rx) = mpsc::loose_bounded(cap);
    let fsm = Runner {
        recv: rx,
    };
    (tx, Box::new(fsm))
}

pub struct Handler;

impl PollHandler<Runner, Runner> for Handler {
    fn begin(&mut self, _batch_size: usize) {
    }

    fn handle_control(&mut self, control: &mut Managed<Runner>) -> bool {
        while let Ok(_) = control.recv.try_recv() {
        }
        false
    }

    fn handle_normal(&mut self, normal: &mut Managed<Runner>) -> bool {
        while let Ok(_) = normal.recv.try_recv() {
        }
        false
    }

    fn end(&mut self, _normals: &mut [Managed<Runner>]) {}
}

pub struct Builder;

impl HandlerBuilder<Runner, Runner> for Builder {
    type Handler = Handler;

    fn build(&mut self) -> Handler {
        Handler
    }
}

#[bench]
fn bench_send(b: &mut Bencher) {
    let (control_tx, control_fsm) = new_runner(100000);
    let (router, mut system) = batch_system::create_system(2, 2, control_tx, control_fsm);
    system.spawn("test".to_owned(), Builder);
    let (normal_tx, normal_fsm) = new_runner(100000);
    let normal_box = BasicMailbox::new(normal_tx, normal_fsm);
    router.register(1, normal_box);

    b.iter(|| {
        router.send(1, ()).unwrap();
    });
    system.shutdown();
}