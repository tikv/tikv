pub use minitrace::*;
use protobuf::Message;

pub use tipb::TracingEvent as Event;

#[cfg(feature = "protobuf-codec")]
pub type Key = tipb::TracingPropertyKey;
#[cfg(feature = "prost-codec")]
pub type Key = tipb::tracing_property::Key;

/// Attach a property to the current span
#[inline]
pub fn property(key: Key, value: String) {
    minitrace::property_closure(|| {
        let mut p = tipb::TracingProperty::default();
        p.set_key(key);
        p.set_value(value);

        // All fields are set properly. It's all right to unwrap.
        p.write_to_bytes().unwrap()
    })
}
