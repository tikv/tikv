#![feature(test)]

extern crate test;

use batch_system::*;
use std::borrow::Cow;
use test::*;
use tikv_util::mpsc;

pub type Message = ();

pub struct Runner {
    is_stopped: bool,
    recv: mpsc::Receiver<Message>,
    mailbox: Option<BasicMailbox<Runner>>,
}

impl Fsm for Runner {
    type Message = Message;

    fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>) {
        self.mailbox = Some(mailbox.into_owned());
    }

    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>> {
        self.mailbox.take()
    }
}

pub fn new_runner(cap: usize) -> (mpsc::LooseBoundedSender<Message>, Box<Runner>) {
    let (tx, rx) = mpsc::loose_bounded(cap);
    let fsm = Runner {
        is_stopped: false,
        recv: rx,
        mailbox: None,
    };
    (tx, Box::new(fsm))
}

pub struct Handler;

impl PollHandler<Runner, Runner> for Handler {
    fn begin(&mut self, _batch_size: usize) {}

    fn handle_control(&mut self, control: &mut Runner) -> Option<usize> {
        while let Ok(_) = control.recv.try_recv() {}
        Some(0)
    }

    fn handle_normal(&mut self, normal: &mut Runner) -> Option<usize> {
        while let Ok(_) = normal.recv.try_recv() {}
        Some(0)
    }

    fn end(&mut self, _normals: &mut [Box<Runner>]) {}
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
