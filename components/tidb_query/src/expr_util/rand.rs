const MAX_RAND_VALUE: u32 = 0x3FFFFFFF;

pub struct MySQLRng {
    seed1: u32,
    seed2: u32,
}

impl MySQLRng {
    pub fn new() -> Self {
        let current_time = time::get_time();
        let nsec = i64::from(current_time.nsec);
        Self::new_with_seed(nsec)
    }

    pub fn new_with_seed(seed: i64) -> Self {
        let seed1 = (seed.wrapping_mul(0x10001).wrapping_add(55555555)) as u32 % MAX_RAND_VALUE;
        let seed2 = (seed.wrapping_mul(0x10000001)) as u32 % MAX_RAND_VALUE;
        MySQLRng { seed1, seed2 }
    }

    pub fn gen(&mut self) -> f64 {
        self.seed1 = (self.seed1 * 3 + self.seed2) % MAX_RAND_VALUE;
        self.seed2 = (self.seed1 + self.seed2 + 33) % MAX_RAND_VALUE;
        f64::from(self.seed1) / f64::from(MAX_RAND_VALUE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_rand_new() {
        let mut rng1 = MySQLRng::new();
        let mut rng2 = MySQLRng::new();
        let got1 = rng1.gen();
        let got2 = rng2.gen();
        assert!(got1 < 1.0);
        assert!(got1 >= 0.0);
        assert_ne!(got1, rng1.gen());
        assert!(got2 < 1.0);
        assert!(got2 >= 0.0);
        assert_ne!(got2, rng2.gen());
        assert_ne!(got1, got2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_rand_new_with_seed() {
        let tests = vec![
            (0, 0.15522042769493574, 0.620881741513388),
            (1, 0.40540353712197724, 0.8716141803857071),
            (-1, 0.9050373219931845, 0.37014932126752037),
            (9223372036854775807, 0.9050373219931845, 0.37014932126752037),
        ];
        for (seed, exp1, exp2) in tests {
            let mut rand = MySQLRng::new_with_seed(seed);
            let res1 = rand.gen();
            assert_eq!(res1, exp1);
            let res2 = rand.gen();
            assert_eq!(res2, exp2);
        }
    }
}
