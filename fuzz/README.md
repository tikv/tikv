# Fuzz testing

Fuzz testing for TiKV.

Currently, it targets in following components:

 - `tikv::util::codec::bytes`
 - `tikv::util::codec::number`
 - `tikv::coprocessor::codec::mysql::decimal::Decimal`

To run fuzz testing, you need to install `cargo-fuzz`:

```sh
$ cargo install cargo-fuzz
```

Then pick a target:

```sh
$ cargo fuzz run codec_bytes
```
