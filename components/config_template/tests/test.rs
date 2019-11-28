use config_template::Configable;

#[derive(Configable)]
pub struct MyConfig {
    split_region_on_table: String,
    #[config(not_support)]
    batch_split_limit: u64,
    #[config(sub_modulep)]
    txn_cfg: TxnConfig,
    region_split_keys: u64,
}

// #[derive(Configable)]
// enum Conf {
//     A(u64),
//     B(u64),
// }

#[derive(Configable)]
pub struct TxnConfig {
    #[config(not_support)]
    enabled: bool,
    wait_for_lock_timeout: u64,
    wake_up_delay_duration: u64,
}

fn main() {
    let cfg = MyConfig {
        split_region_on_table: "truee3".to_owned(),
        batch_split_limit: 10,
        txn_cfg: TxnConfig {
            enabled: true,
            wait_for_lock_timeout: 12,
            wake_up_delay_duration: 64,
        },
        region_split_keys: 22,
    };
}
