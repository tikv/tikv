mod stats;

use stats::{StatsMap, TaskStats};

#[derive(Clone)]
pub struct MultilevelPool {
    stats: StatsMap,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
