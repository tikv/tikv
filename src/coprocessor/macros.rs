macro_rules! match_template_evaluable {
    ($t:tt, $($tail:tt)*) => {
        match_template! {
            $t = [Int, Real, Decimal, Bytes, DateTime, Duration, Json],
            $($tail)*
        }
    };
}
