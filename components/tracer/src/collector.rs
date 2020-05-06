#[derive(Clone, Copy, Debug)]
pub enum CollectorType {
    Void,
    Channel,
}

#[derive(Clone, Debug)]
pub enum CollectorTx {
    Void,
    Channel(crossbeam::channel::Sender<crate::Span>),
}
impl CollectorTx {
    #[inline]
    pub fn push(&self, span: crate::Span) {
        match self {
            CollectorTx::Void => (),
            CollectorTx::Channel(c) => {
                let _ = c.try_send(span);
            }
        }
    }
}

pub enum CollectorRx {
    Void,
    Channel(crossbeam::channel::Receiver<crate::Span>),
}
impl CollectorRx {
    #[inline]
    pub fn collect_all(self) -> Vec<crate::Span> {
        match self {
            CollectorRx::Void => vec![],
            CollectorRx::Channel(c) => c.iter().collect(),
        }
    }
}

pub struct Collector {
    pub tx: CollectorTx,
    pub rx: CollectorRx,
}
impl Collector {
    pub fn new(tp: CollectorType) -> Self {
        match tp {
            CollectorType::Void => Collector {
                tx: CollectorTx::Void,
                rx: CollectorRx::Void,
            },
            CollectorType::Channel => {
                let (tx, rx) = crossbeam::channel::unbounded();
                Collector {
                    tx: CollectorTx::Channel(tx),
                    rx: CollectorRx::Channel(rx),
                }
            }
        }
    }
}
