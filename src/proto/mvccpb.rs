// This file is generated. Do not edit
// @generated

#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_imports)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(Clone,Default)]
pub struct MetaItem {
    // message fields
    start_ts: ::std::option::Option<u64>,
    commit_ts: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for MetaItem {}

impl MetaItem {
    pub fn new() -> MetaItem {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static MetaItem {
        static mut instance: ::protobuf::lazy::Lazy<MetaItem> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const MetaItem,
        };
        unsafe {
            instance.get(|| {
                MetaItem {
                    start_ts: ::std::option::Option::None,
                    commit_ts: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional uint64 start_ts = 1;

    pub fn clear_start_ts(&mut self) {
        self.start_ts = ::std::option::Option::None;
    }

    pub fn has_start_ts(&self) -> bool {
        self.start_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_ts(&mut self, v: u64) {
        self.start_ts = ::std::option::Option::Some(v);
    }

    pub fn get_start_ts<'a>(&self) -> u64 {
        self.start_ts.unwrap_or(0)
    }

    // optional uint64 commit_ts = 2;

    pub fn clear_commit_ts(&mut self) {
        self.commit_ts = ::std::option::Option::None;
    }

    pub fn has_commit_ts(&self) -> bool {
        self.commit_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_commit_ts(&mut self, v: u64) {
        self.commit_ts = ::std::option::Option::Some(v);
    }

    pub fn get_commit_ts<'a>(&self) -> u64 {
        self.commit_ts.unwrap_or(0)
    }
}

impl ::protobuf::Message for MetaItem {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.commit_ts = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.start_ts.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.commit_ts.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.start_ts {
            try!(os.write_uint64(1, v));
        };
        if let Some(v) = self.commit_ts {
            try!(os.write_uint64(2, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<MetaItem>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for MetaItem {
    fn new() -> MetaItem {
        MetaItem::new()
    }

    fn descriptor_static(_: ::std::option::Option<MetaItem>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "start_ts",
                    MetaItem::has_start_ts,
                    MetaItem::get_start_ts,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "commit_ts",
                    MetaItem::has_commit_ts,
                    MetaItem::get_commit_ts,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<MetaItem>(
                    "MetaItem",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for MetaItem {
    fn clear(&mut self) {
        self.clear_start_ts();
        self.clear_commit_ts();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for MetaItem {
    fn eq(&self, other: &MetaItem) -> bool {
        self.start_ts == other.start_ts &&
        self.commit_ts == other.commit_ts &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for MetaItem {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct MetaLock {
    // message fields
    field_type: ::std::option::Option<MetaLockType>,
    start_ts: ::std::option::Option<u64>,
    primary_key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for MetaLock {}

impl MetaLock {
    pub fn new() -> MetaLock {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static MetaLock {
        static mut instance: ::protobuf::lazy::Lazy<MetaLock> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const MetaLock,
        };
        unsafe {
            instance.get(|| {
                MetaLock {
                    field_type: ::std::option::Option::None,
                    start_ts: ::std::option::Option::None,
                    primary_key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .mvccpb.MetaLockType type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: MetaLockType) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type<'a>(&self) -> MetaLockType {
        self.field_type.unwrap_or(MetaLockType::ReadOnly)
    }

    // optional uint64 start_ts = 2;

    pub fn clear_start_ts(&mut self) {
        self.start_ts = ::std::option::Option::None;
    }

    pub fn has_start_ts(&self) -> bool {
        self.start_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_ts(&mut self, v: u64) {
        self.start_ts = ::std::option::Option::Some(v);
    }

    pub fn get_start_ts<'a>(&self) -> u64 {
        self.start_ts.unwrap_or(0)
    }

    // optional bytes primary_key = 3;

    pub fn clear_primary_key(&mut self) {
        self.primary_key.clear();
    }

    pub fn has_primary_key(&self) -> bool {
        self.primary_key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_primary_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.primary_key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_primary_key<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.primary_key.is_none() {
            self.primary_key.set_default();
        };
        self.primary_key.as_mut().unwrap()
    }

    // Take field
    pub fn take_primary_key(&mut self) -> ::std::vec::Vec<u8> {
        self.primary_key.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_primary_key<'a>(&'a self) -> &'a [u8] {
        match self.primary_key.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for MetaLock {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.field_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                3 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.primary_key));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.field_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.start_ts.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.primary_key.iter() {
            my_size += ::protobuf::rt::bytes_size(3, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.start_ts {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.primary_key.as_ref() {
            try!(os.write_bytes(3, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<MetaLock>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for MetaLock {
    fn new() -> MetaLock {
        MetaLock::new()
    }

    fn descriptor_static(_: ::std::option::Option<MetaLock>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "type",
                    MetaLock::has_field_type,
                    MetaLock::get_field_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "start_ts",
                    MetaLock::has_start_ts,
                    MetaLock::get_start_ts,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "primary_key",
                    MetaLock::has_primary_key,
                    MetaLock::get_primary_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<MetaLock>(
                    "MetaLock",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for MetaLock {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_start_ts();
        self.clear_primary_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for MetaLock {
    fn eq(&self, other: &MetaLock) -> bool {
        self.field_type == other.field_type &&
        self.start_ts == other.start_ts &&
        self.primary_key == other.primary_key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for MetaLock {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Meta {
    // message fields
    lock: ::protobuf::SingularPtrField<MetaLock>,
    items: ::protobuf::RepeatedField<MetaItem>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Meta {}

impl Meta {
    pub fn new() -> Meta {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Meta {
        static mut instance: ::protobuf::lazy::Lazy<Meta> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Meta,
        };
        unsafe {
            instance.get(|| {
                Meta {
                    lock: ::protobuf::SingularPtrField::none(),
                    items: ::protobuf::RepeatedField::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .mvccpb.MetaLock lock = 1;

    pub fn clear_lock(&mut self) {
        self.lock.clear();
    }

    pub fn has_lock(&self) -> bool {
        self.lock.is_some()
    }

    // Param is passed by value, moved
    pub fn set_lock(&mut self, v: MetaLock) {
        self.lock = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_lock<'a>(&'a mut self) -> &'a mut MetaLock {
        if self.lock.is_none() {
            self.lock.set_default();
        };
        self.lock.as_mut().unwrap()
    }

    // Take field
    pub fn take_lock(&mut self) -> MetaLock {
        self.lock.take().unwrap_or_else(|| MetaLock::new())
    }

    pub fn get_lock<'a>(&'a self) -> &'a MetaLock {
        self.lock.as_ref().unwrap_or_else(|| MetaLock::default_instance())
    }

    // repeated .mvccpb.MetaItem items = 2;

    pub fn clear_items(&mut self) {
        self.items.clear();
    }

    // Param is passed by value, moved
    pub fn set_items(&mut self, v: ::protobuf::RepeatedField<MetaItem>) {
        self.items = v;
    }

    // Mutable pointer to the field.
    pub fn mut_items<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<MetaItem> {
        &mut self.items
    }

    // Take field
    pub fn take_items(&mut self) -> ::protobuf::RepeatedField<MetaItem> {
        ::std::mem::replace(&mut self.items, ::protobuf::RepeatedField::new())
    }

    pub fn get_items<'a>(&'a self) -> &'a [MetaItem] {
        &self.items
    }
}

impl ::protobuf::Message for Meta {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.lock));
                },
                2 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.items));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.lock.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.items.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.lock.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        for v in self.items.iter() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Meta>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Meta {
    fn new() -> Meta {
        Meta::new()
    }

    fn descriptor_static(_: ::std::option::Option<Meta>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "lock",
                    Meta::has_lock,
                    Meta::get_lock,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "items",
                    Meta::get_items,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Meta>(
                    "Meta",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Meta {
    fn clear(&mut self) {
        self.clear_lock();
        self.clear_items();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Meta {
    fn eq(&self, other: &Meta) -> bool {
        self.lock == other.lock &&
        self.items == other.items &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Meta {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum MetaLockType {
    ReadOnly = 1,
    ReadWrite = 2,
}

impl ::protobuf::ProtobufEnum for MetaLockType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<MetaLockType> {
        match value {
            1 => ::std::option::Option::Some(MetaLockType::ReadOnly),
            2 => ::std::option::Option::Some(MetaLockType::ReadWrite),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [MetaLockType] = &[
            MetaLockType::ReadOnly,
            MetaLockType::ReadWrite,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<MetaLockType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("MetaLockType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for MetaLockType {
}

static file_descriptor_proto_data: &'static [u8] = &[
    0x0a, 0x0c, 0x6d, 0x76, 0x63, 0x63, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
    0x6d, 0x76, 0x63, 0x63, 0x70, 0x62, 0x22, 0x2f, 0x0a, 0x08, 0x4d, 0x65, 0x74, 0x61, 0x49, 0x74,
    0x65, 0x6d, 0x12, 0x10, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x73, 0x18, 0x01,
    0x20, 0x01, 0x28, 0x04, 0x12, 0x11, 0x0a, 0x09, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x5f, 0x74,
    0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x22, 0x55, 0x0a, 0x08, 0x4d, 0x65, 0x74, 0x61, 0x4c,
    0x6f, 0x63, 0x6b, 0x12, 0x22, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0e, 0x32, 0x14, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x70, 0x62, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x4c,
    0x6f, 0x63, 0x6b, 0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74,
    0x5f, 0x74, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x12, 0x13, 0x0a, 0x0b, 0x70, 0x72, 0x69,
    0x6d, 0x61, 0x72, 0x79, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x22, 0x47,
    0x0a, 0x04, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x1e, 0x0a, 0x04, 0x6c, 0x6f, 0x63, 0x6b, 0x18, 0x01,
    0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x70, 0x62, 0x2e, 0x4d, 0x65,
    0x74, 0x61, 0x4c, 0x6f, 0x63, 0x6b, 0x12, 0x1f, 0x0a, 0x05, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x18,
    0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x6d, 0x76, 0x63, 0x63, 0x70, 0x62, 0x2e, 0x4d,
    0x65, 0x74, 0x61, 0x49, 0x74, 0x65, 0x6d, 0x2a, 0x2b, 0x0a, 0x0c, 0x4d, 0x65, 0x74, 0x61, 0x4c,
    0x6f, 0x63, 0x6b, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0c, 0x0a, 0x08, 0x52, 0x65, 0x61, 0x64, 0x4f,
    0x6e, 0x6c, 0x79, 0x10, 0x01, 0x12, 0x0d, 0x0a, 0x09, 0x52, 0x65, 0x61, 0x64, 0x57, 0x72, 0x69,
    0x74, 0x65, 0x10, 0x02, 0x4a, 0xa7, 0x05, 0x0a, 0x06, 0x12, 0x04, 0x00, 0x00, 0x16, 0x01, 0x0a,
    0x08, 0x0a, 0x01, 0x02, 0x12, 0x03, 0x01, 0x08, 0x0e, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x00, 0x12,
    0x04, 0x03, 0x00, 0x06, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x00, 0x01, 0x12, 0x03, 0x03, 0x08,
    0x10, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x00, 0x12, 0x03, 0x04, 0x04, 0x23, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x04, 0x12, 0x03, 0x04, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x00, 0x02, 0x00, 0x05, 0x12, 0x03, 0x04, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
    0x02, 0x00, 0x01, 0x12, 0x03, 0x04, 0x14, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00,
    0x03, 0x12, 0x03, 0x04, 0x21, 0x22, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x01, 0x12, 0x03,
    0x05, 0x04, 0x23, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x04, 0x12, 0x03, 0x05, 0x04,
    0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x05, 0x12, 0x03, 0x05, 0x0d, 0x13, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x01, 0x12, 0x03, 0x05, 0x14, 0x1d, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x00, 0x02, 0x01, 0x03, 0x12, 0x03, 0x05, 0x21, 0x22, 0x0a, 0x0a, 0x0a, 0x02, 0x05,
    0x00, 0x12, 0x04, 0x08, 0x00, 0x0b, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x00, 0x01, 0x12, 0x03,
    0x08, 0x05, 0x11, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x00, 0x12, 0x03, 0x09, 0x04, 0x12,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x00, 0x01, 0x12, 0x03, 0x09, 0x04, 0x0c, 0x0a, 0x0c,
    0x0a, 0x05, 0x05, 0x00, 0x02, 0x00, 0x02, 0x12, 0x03, 0x09, 0x10, 0x11, 0x0a, 0x0b, 0x0a, 0x04,
    0x05, 0x00, 0x02, 0x01, 0x12, 0x03, 0x0a, 0x04, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
    0x01, 0x01, 0x12, 0x03, 0x0a, 0x04, 0x0d, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x02,
    0x12, 0x03, 0x0a, 0x10, 0x11, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x01, 0x12, 0x04, 0x0d, 0x00, 0x11,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x01, 0x01, 0x12, 0x03, 0x0d, 0x08, 0x10, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x01, 0x02, 0x00, 0x12, 0x03, 0x0e, 0x04, 0x2a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01,
    0x02, 0x00, 0x04, 0x12, 0x03, 0x0e, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00,
    0x06, 0x12, 0x03, 0x0e, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x01, 0x12,
    0x03, 0x0e, 0x1a, 0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x03, 0x12, 0x03, 0x0e,
    0x28, 0x29, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x01, 0x02, 0x01, 0x12, 0x03, 0x0f, 0x04, 0x2a, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x01, 0x04, 0x12, 0x03, 0x0f, 0x04, 0x0c, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x01, 0x02, 0x01, 0x05, 0x12, 0x03, 0x0f, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x01, 0x02, 0x01, 0x01, 0x12, 0x03, 0x0f, 0x1a, 0x22, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x0f, 0x28, 0x29, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x01, 0x02, 0x02, 0x12,
    0x03, 0x10, 0x04, 0x2a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x02, 0x04, 0x12, 0x03, 0x10,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x02, 0x05, 0x12, 0x03, 0x10, 0x0d, 0x12,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x02, 0x01, 0x12, 0x03, 0x10, 0x1a, 0x25, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x01, 0x02, 0x02, 0x03, 0x12, 0x03, 0x10, 0x28, 0x29, 0x0a, 0x0a, 0x0a, 0x02,
    0x04, 0x02, 0x12, 0x04, 0x13, 0x00, 0x16, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x02, 0x01, 0x12,
    0x03, 0x13, 0x08, 0x0c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02, 0x00, 0x12, 0x03, 0x14, 0x04,
    0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x04, 0x12, 0x03, 0x14, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x06, 0x12, 0x03, 0x14, 0x0d, 0x15, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x02, 0x02, 0x00, 0x01, 0x12, 0x03, 0x14, 0x16, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x02, 0x02, 0x00, 0x03, 0x12, 0x03, 0x14, 0x24, 0x25, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02,
    0x01, 0x12, 0x03, 0x15, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x04, 0x12,
    0x03, 0x15, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x06, 0x12, 0x03, 0x15,
    0x0d, 0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x01, 0x12, 0x03, 0x15, 0x16, 0x1b,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x03, 0x12, 0x03, 0x15, 0x24, 0x25,
];

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
