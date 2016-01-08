use self::engine::Engine;
pub use self::engine::Descriptor;

mod engine;

pub struct Storage {
    engine: Box<Engine>,
}

impl Storage {
    pub fn new(desc: Descriptor) -> Result<Storage, String> {
        engine::new_engine(desc).map(|e| Storage { engine: e })
    }

    pub fn get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>, String> {
        trace!("storage: get {:?}@{}", key, version);
        // TODO (disksing)
        self.engine.get(key)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], version: u64) -> Result<(), String> {
        trace!("storage: put {:?}@{}", key, version);
        // TODO (disksing)
        self.engine.put(key, value)
    }
}
