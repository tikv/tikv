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
pub struct Entry {
    // message fields
    Type: ::std::option::Option<EntryType>,
    Term: ::std::option::Option<u64>,
    Index: ::std::option::Option<u64>,
    Data: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl Entry {
    pub fn new() -> Entry {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Entry {
        static mut instance: ::protobuf::lazy::Lazy<Entry> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Entry,
        };
        unsafe {
            instance.get(|| {
                Entry {
                    Type: ::std::option::Option::None,
                    Term: ::std::option::Option::None,
                    Index: ::std::option::Option::None,
                    Data: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raftpb.EntryType Type = 1;

    pub fn clear_Type(&mut self) {
        self.Type = ::std::option::Option::None;
    }

    pub fn has_Type(&self) -> bool {
        self.Type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_Type(&mut self, v: EntryType) {
        self.Type = ::std::option::Option::Some(v);
    }

    pub fn get_Type<'a>(&self) -> EntryType {
        self.Type.unwrap_or(EntryType::EntryNormal)
    }

    // optional uint64 Term = 2;

    pub fn clear_Term(&mut self) {
        self.Term = ::std::option::Option::None;
    }

    pub fn has_Term(&self) -> bool {
        self.Term.is_some()
    }

    // Param is passed by value, moved
    pub fn set_Term(&mut self, v: u64) {
        self.Term = ::std::option::Option::Some(v);
    }

    pub fn get_Term<'a>(&self) -> u64 {
        self.Term.unwrap_or(0)
    }

    // optional uint64 Index = 3;

    pub fn clear_Index(&mut self) {
        self.Index = ::std::option::Option::None;
    }

    pub fn has_Index(&self) -> bool {
        self.Index.is_some()
    }

    // Param is passed by value, moved
    pub fn set_Index(&mut self, v: u64) {
        self.Index = ::std::option::Option::Some(v);
    }

    pub fn get_Index<'a>(&self) -> u64 {
        self.Index.unwrap_or(0)
    }

    // optional bytes Data = 4;

    pub fn clear_Data(&mut self) {
        self.Data.clear();
    }

    pub fn has_Data(&self) -> bool {
        self.Data.is_some()
    }

    // Param is passed by value, moved
    pub fn set_Data(&mut self, v: ::std::vec::Vec<u8>) {
        self.Data = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_Data<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.Data.is_none() {
            self.Data.set_default();
        };
        self.Data.as_mut().unwrap()
    }

    // Take field
    pub fn take_Data(&mut self) -> ::std::vec::Vec<u8> {
        self.Data.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_Data<'a>(&'a self) -> &'a [u8] {
        match self.Data.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for Entry {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_enum());
                    self.Type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.Term = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.Index = ::std::option::Option::Some(tmp);
                },
                4 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.Data));
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.Type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.Term.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.Index.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.Data.iter() {
            my_size += ::protobuf::rt::bytes_size(4, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.Type {
            try!(os.write_enum(1, v as i32));
        };
        if let Some(v) = self.Term {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.Index {
            try!(os.write_uint64(3, v));
        };
        if let Some(v) = self.Data.as_ref() {
            try!(os.write_bytes(4, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Entry>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Entry {
    fn new() -> Entry {
        Entry::new()
    }

    fn descriptor_static(_: ::std::option::Option<Entry>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "Type",
                    Entry::has_Type,
                    Entry::get_Type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "Term",
                    Entry::has_Term,
                    Entry::get_Term,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "Index",
                    Entry::has_Index,
                    Entry::get_Index,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "Data",
                    Entry::has_Data,
                    Entry::get_Data,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Entry>(
                    "Entry",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Entry {
    fn clear(&mut self) {
        self.clear_Type();
        self.clear_Term();
        self.clear_Index();
        self.clear_Data();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        self.Type == other.Type &&
        self.Term == other.Term &&
        self.Index == other.Index &&
        self.Data == other.Data &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct SnapshotMetadata {
    // message fields
    conf_state: ::protobuf::SingularPtrField<ConfState>,
    index: ::std::option::Option<u64>,
    term: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl SnapshotMetadata {
    pub fn new() -> SnapshotMetadata {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SnapshotMetadata {
        static mut instance: ::protobuf::lazy::Lazy<SnapshotMetadata> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SnapshotMetadata,
        };
        unsafe {
            instance.get(|| {
                SnapshotMetadata {
                    conf_state: ::protobuf::SingularPtrField::none(),
                    index: ::std::option::Option::None,
                    term: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raftpb.ConfState conf_state = 1;

    pub fn clear_conf_state(&mut self) {
        self.conf_state.clear();
    }

    pub fn has_conf_state(&self) -> bool {
        self.conf_state.is_some()
    }

    // Param is passed by value, moved
    pub fn set_conf_state(&mut self, v: ConfState) {
        self.conf_state = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_conf_state<'a>(&'a mut self) -> &'a mut ConfState {
        if self.conf_state.is_none() {
            self.conf_state.set_default();
        };
        self.conf_state.as_mut().unwrap()
    }

    // Take field
    pub fn take_conf_state(&mut self) -> ConfState {
        self.conf_state.take().unwrap_or_else(|| ConfState::new())
    }

    pub fn get_conf_state<'a>(&'a self) -> &'a ConfState {
        self.conf_state.as_ref().unwrap_or_else(|| ConfState::default_instance())
    }

    // optional uint64 index = 2;

    pub fn clear_index(&mut self) {
        self.index = ::std::option::Option::None;
    }

    pub fn has_index(&self) -> bool {
        self.index.is_some()
    }

    // Param is passed by value, moved
    pub fn set_index(&mut self, v: u64) {
        self.index = ::std::option::Option::Some(v);
    }

    pub fn get_index<'a>(&self) -> u64 {
        self.index.unwrap_or(0)
    }

    // optional uint64 term = 3;

    pub fn clear_term(&mut self) {
        self.term = ::std::option::Option::None;
    }

    pub fn has_term(&self) -> bool {
        self.term.is_some()
    }

    // Param is passed by value, moved
    pub fn set_term(&mut self, v: u64) {
        self.term = ::std::option::Option::Some(v);
    }

    pub fn get_term<'a>(&self) -> u64 {
        self.term.unwrap_or(0)
    }
}

impl ::protobuf::Message for SnapshotMetadata {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.conf_state));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.index = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.term = ::std::option::Option::Some(tmp);
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.conf_state.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.index.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.term.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.conf_state.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.index {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.term {
            try!(os.write_uint64(3, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<SnapshotMetadata>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SnapshotMetadata {
    fn new() -> SnapshotMetadata {
        SnapshotMetadata::new()
    }

    fn descriptor_static(_: ::std::option::Option<SnapshotMetadata>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "conf_state",
                    SnapshotMetadata::has_conf_state,
                    SnapshotMetadata::get_conf_state,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "index",
                    SnapshotMetadata::has_index,
                    SnapshotMetadata::get_index,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "term",
                    SnapshotMetadata::has_term,
                    SnapshotMetadata::get_term,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SnapshotMetadata>(
                    "SnapshotMetadata",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SnapshotMetadata {
    fn clear(&mut self) {
        self.clear_conf_state();
        self.clear_index();
        self.clear_term();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for SnapshotMetadata {
    fn eq(&self, other: &SnapshotMetadata) -> bool {
        self.conf_state == other.conf_state &&
        self.index == other.index &&
        self.term == other.term &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for SnapshotMetadata {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Snapshot {
    // message fields
    data: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    metadata: ::protobuf::SingularPtrField<SnapshotMetadata>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl Snapshot {
    pub fn new() -> Snapshot {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Snapshot {
        static mut instance: ::protobuf::lazy::Lazy<Snapshot> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Snapshot,
        };
        unsafe {
            instance.get(|| {
                Snapshot {
                    data: ::protobuf::SingularField::none(),
                    metadata: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes data = 1;

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    // Param is passed by value, moved
    pub fn set_data(&mut self, v: ::std::vec::Vec<u8>) {
        self.data = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_data<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.data.is_none() {
            self.data.set_default();
        };
        self.data.as_mut().unwrap()
    }

    // Take field
    pub fn take_data(&mut self) -> ::std::vec::Vec<u8> {
        self.data.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_data<'a>(&'a self) -> &'a [u8] {
        match self.data.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    // optional .raftpb.SnapshotMetadata metadata = 2;

    pub fn clear_metadata(&mut self) {
        self.metadata.clear();
    }

    pub fn has_metadata(&self) -> bool {
        self.metadata.is_some()
    }

    // Param is passed by value, moved
    pub fn set_metadata(&mut self, v: SnapshotMetadata) {
        self.metadata = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_metadata<'a>(&'a mut self) -> &'a mut SnapshotMetadata {
        if self.metadata.is_none() {
            self.metadata.set_default();
        };
        self.metadata.as_mut().unwrap()
    }

    // Take field
    pub fn take_metadata(&mut self) -> SnapshotMetadata {
        self.metadata.take().unwrap_or_else(|| SnapshotMetadata::new())
    }

    pub fn get_metadata<'a>(&'a self) -> &'a SnapshotMetadata {
        self.metadata.as_ref().unwrap_or_else(|| SnapshotMetadata::default_instance())
    }
}

impl ::protobuf::Message for Snapshot {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.data));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.metadata));
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.data.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        for value in self.metadata.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.data.as_ref() {
            try!(os.write_bytes(1, &v));
        };
        if let Some(v) = self.metadata.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Snapshot>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Snapshot {
    fn new() -> Snapshot {
        Snapshot::new()
    }

    fn descriptor_static(_: ::std::option::Option<Snapshot>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "data",
                    Snapshot::has_data,
                    Snapshot::get_data,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "metadata",
                    Snapshot::has_metadata,
                    Snapshot::get_metadata,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Snapshot>(
                    "Snapshot",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Snapshot {
    fn clear(&mut self) {
        self.clear_data();
        self.clear_metadata();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Snapshot {
    fn eq(&self, other: &Snapshot) -> bool {
        self.data == other.data &&
        self.metadata == other.metadata &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Message {
    // message fields
    field_type: ::std::option::Option<MessageType>,
    to: ::std::option::Option<u64>,
    from: ::std::option::Option<u64>,
    term: ::std::option::Option<u64>,
    logTerm: ::std::option::Option<u64>,
    index: ::std::option::Option<u64>,
    entries: ::protobuf::RepeatedField<Entry>,
    commit: ::std::option::Option<u64>,
    snapshot: ::protobuf::SingularPtrField<Snapshot>,
    reject: ::std::option::Option<bool>,
    rejectHint: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl Message {
    pub fn new() -> Message {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Message {
        static mut instance: ::protobuf::lazy::Lazy<Message> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Message,
        };
        unsafe {
            instance.get(|| {
                Message {
                    field_type: ::std::option::Option::None,
                    to: ::std::option::Option::None,
                    from: ::std::option::Option::None,
                    term: ::std::option::Option::None,
                    logTerm: ::std::option::Option::None,
                    index: ::std::option::Option::None,
                    entries: ::protobuf::RepeatedField::new(),
                    commit: ::std::option::Option::None,
                    snapshot: ::protobuf::SingularPtrField::none(),
                    reject: ::std::option::Option::None,
                    rejectHint: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raftpb.MessageType type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: MessageType) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type<'a>(&self) -> MessageType {
        self.field_type.unwrap_or(MessageType::MsgHup)
    }

    // optional uint64 to = 2;

    pub fn clear_to(&mut self) {
        self.to = ::std::option::Option::None;
    }

    pub fn has_to(&self) -> bool {
        self.to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_to(&mut self, v: u64) {
        self.to = ::std::option::Option::Some(v);
    }

    pub fn get_to<'a>(&self) -> u64 {
        self.to.unwrap_or(0)
    }

    // optional uint64 from = 3;

    pub fn clear_from(&mut self) {
        self.from = ::std::option::Option::None;
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: u64) {
        self.from = ::std::option::Option::Some(v);
    }

    pub fn get_from<'a>(&self) -> u64 {
        self.from.unwrap_or(0)
    }

    // optional uint64 term = 4;

    pub fn clear_term(&mut self) {
        self.term = ::std::option::Option::None;
    }

    pub fn has_term(&self) -> bool {
        self.term.is_some()
    }

    // Param is passed by value, moved
    pub fn set_term(&mut self, v: u64) {
        self.term = ::std::option::Option::Some(v);
    }

    pub fn get_term<'a>(&self) -> u64 {
        self.term.unwrap_or(0)
    }

    // optional uint64 logTerm = 5;

    pub fn clear_logTerm(&mut self) {
        self.logTerm = ::std::option::Option::None;
    }

    pub fn has_logTerm(&self) -> bool {
        self.logTerm.is_some()
    }

    // Param is passed by value, moved
    pub fn set_logTerm(&mut self, v: u64) {
        self.logTerm = ::std::option::Option::Some(v);
    }

    pub fn get_logTerm<'a>(&self) -> u64 {
        self.logTerm.unwrap_or(0)
    }

    // optional uint64 index = 6;

    pub fn clear_index(&mut self) {
        self.index = ::std::option::Option::None;
    }

    pub fn has_index(&self) -> bool {
        self.index.is_some()
    }

    // Param is passed by value, moved
    pub fn set_index(&mut self, v: u64) {
        self.index = ::std::option::Option::Some(v);
    }

    pub fn get_index<'a>(&self) -> u64 {
        self.index.unwrap_or(0)
    }

    // repeated .raftpb.Entry entries = 7;

    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }

    // Param is passed by value, moved
    pub fn set_entries(&mut self, v: ::protobuf::RepeatedField<Entry>) {
        self.entries = v;
    }

    // Mutable pointer to the field.
    pub fn mut_entries<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<Entry> {
        &mut self.entries
    }

    // Take field
    pub fn take_entries(&mut self) -> ::protobuf::RepeatedField<Entry> {
        ::std::mem::replace(&mut self.entries, ::protobuf::RepeatedField::new())
    }

    pub fn get_entries<'a>(&'a self) -> &'a [Entry] {
        &self.entries
    }

    // optional uint64 commit = 8;

    pub fn clear_commit(&mut self) {
        self.commit = ::std::option::Option::None;
    }

    pub fn has_commit(&self) -> bool {
        self.commit.is_some()
    }

    // Param is passed by value, moved
    pub fn set_commit(&mut self, v: u64) {
        self.commit = ::std::option::Option::Some(v);
    }

    pub fn get_commit<'a>(&self) -> u64 {
        self.commit.unwrap_or(0)
    }

    // optional .raftpb.Snapshot snapshot = 9;

    pub fn clear_snapshot(&mut self) {
        self.snapshot.clear();
    }

    pub fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }

    // Param is passed by value, moved
    pub fn set_snapshot(&mut self, v: Snapshot) {
        self.snapshot = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_snapshot<'a>(&'a mut self) -> &'a mut Snapshot {
        if self.snapshot.is_none() {
            self.snapshot.set_default();
        };
        self.snapshot.as_mut().unwrap()
    }

    // Take field
    pub fn take_snapshot(&mut self) -> Snapshot {
        self.snapshot.take().unwrap_or_else(|| Snapshot::new())
    }

    pub fn get_snapshot<'a>(&'a self) -> &'a Snapshot {
        self.snapshot.as_ref().unwrap_or_else(|| Snapshot::default_instance())
    }

    // optional bool reject = 10;

    pub fn clear_reject(&mut self) {
        self.reject = ::std::option::Option::None;
    }

    pub fn has_reject(&self) -> bool {
        self.reject.is_some()
    }

    // Param is passed by value, moved
    pub fn set_reject(&mut self, v: bool) {
        self.reject = ::std::option::Option::Some(v);
    }

    pub fn get_reject<'a>(&self) -> bool {
        self.reject.unwrap_or(false)
    }

    // optional uint64 rejectHint = 11;

    pub fn clear_rejectHint(&mut self) {
        self.rejectHint = ::std::option::Option::None;
    }

    pub fn has_rejectHint(&self) -> bool {
        self.rejectHint.is_some()
    }

    // Param is passed by value, moved
    pub fn set_rejectHint(&mut self, v: u64) {
        self.rejectHint = ::std::option::Option::Some(v);
    }

    pub fn get_rejectHint<'a>(&self) -> u64 {
        self.rejectHint.unwrap_or(0)
    }
}

impl ::protobuf::Message for Message {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_enum());
                    self.field_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.to = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.from = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.term = ::std::option::Option::Some(tmp);
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.logTerm = ::std::option::Option::Some(tmp);
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.index = ::std::option::Option::Some(tmp);
                },
                7 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.entries));
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.commit = ::std::option::Option::Some(tmp);
                },
                9 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.snapshot));
                },
                10 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_bool());
                    self.reject = ::std::option::Option::Some(tmp);
                },
                11 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.rejectHint = ::std::option::Option::Some(tmp);
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
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
        for value in self.to.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.from.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.term.iter() {
            my_size += ::protobuf::rt::value_size(4, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.logTerm.iter() {
            my_size += ::protobuf::rt::value_size(5, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.index.iter() {
            my_size += ::protobuf::rt::value_size(6, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.entries.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.commit.iter() {
            my_size += ::protobuf::rt::value_size(8, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.snapshot.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if self.reject.is_some() {
            my_size += 2;
        };
        for value in self.rejectHint.iter() {
            my_size += ::protobuf::rt::value_size(11, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            try!(os.write_enum(1, v as i32));
        };
        if let Some(v) = self.to {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.from {
            try!(os.write_uint64(3, v));
        };
        if let Some(v) = self.term {
            try!(os.write_uint64(4, v));
        };
        if let Some(v) = self.logTerm {
            try!(os.write_uint64(5, v));
        };
        if let Some(v) = self.index {
            try!(os.write_uint64(6, v));
        };
        for v in self.entries.iter() {
            try!(os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.commit {
            try!(os.write_uint64(8, v));
        };
        if let Some(v) = self.snapshot.as_ref() {
            try!(os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.reject {
            try!(os.write_bool(10, v));
        };
        if let Some(v) = self.rejectHint {
            try!(os.write_uint64(11, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Message>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Message {
    fn new() -> Message {
        Message::new()
    }

    fn descriptor_static(_: ::std::option::Option<Message>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "type",
                    Message::has_field_type,
                    Message::get_field_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "to",
                    Message::has_to,
                    Message::get_to,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "from",
                    Message::has_from,
                    Message::get_from,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "term",
                    Message::has_term,
                    Message::get_term,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "logTerm",
                    Message::has_logTerm,
                    Message::get_logTerm,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "index",
                    Message::has_index,
                    Message::get_index,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "entries",
                    Message::get_entries,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "commit",
                    Message::has_commit,
                    Message::get_commit,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "snapshot",
                    Message::has_snapshot,
                    Message::get_snapshot,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "reject",
                    Message::has_reject,
                    Message::get_reject,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "rejectHint",
                    Message::has_rejectHint,
                    Message::get_rejectHint,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Message>(
                    "Message",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Message {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_to();
        self.clear_from();
        self.clear_term();
        self.clear_logTerm();
        self.clear_index();
        self.clear_entries();
        self.clear_commit();
        self.clear_snapshot();
        self.clear_reject();
        self.clear_rejectHint();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Message {
    fn eq(&self, other: &Message) -> bool {
        self.field_type == other.field_type &&
        self.to == other.to &&
        self.from == other.from &&
        self.term == other.term &&
        self.logTerm == other.logTerm &&
        self.index == other.index &&
        self.entries == other.entries &&
        self.commit == other.commit &&
        self.snapshot == other.snapshot &&
        self.reject == other.reject &&
        self.rejectHint == other.rejectHint &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Message {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct HardState {
    // message fields
    term: ::std::option::Option<u64>,
    vote: ::std::option::Option<u64>,
    commit: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl HardState {
    pub fn new() -> HardState {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static HardState {
        static mut instance: ::protobuf::lazy::Lazy<HardState> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const HardState,
        };
        unsafe {
            instance.get(|| {
                HardState {
                    term: ::std::option::Option::None,
                    vote: ::std::option::Option::None,
                    commit: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional uint64 term = 1;

    pub fn clear_term(&mut self) {
        self.term = ::std::option::Option::None;
    }

    pub fn has_term(&self) -> bool {
        self.term.is_some()
    }

    // Param is passed by value, moved
    pub fn set_term(&mut self, v: u64) {
        self.term = ::std::option::Option::Some(v);
    }

    pub fn get_term<'a>(&self) -> u64 {
        self.term.unwrap_or(0)
    }

    // optional uint64 vote = 2;

    pub fn clear_vote(&mut self) {
        self.vote = ::std::option::Option::None;
    }

    pub fn has_vote(&self) -> bool {
        self.vote.is_some()
    }

    // Param is passed by value, moved
    pub fn set_vote(&mut self, v: u64) {
        self.vote = ::std::option::Option::Some(v);
    }

    pub fn get_vote<'a>(&self) -> u64 {
        self.vote.unwrap_or(0)
    }

    // optional uint64 commit = 3;

    pub fn clear_commit(&mut self) {
        self.commit = ::std::option::Option::None;
    }

    pub fn has_commit(&self) -> bool {
        self.commit.is_some()
    }

    // Param is passed by value, moved
    pub fn set_commit(&mut self, v: u64) {
        self.commit = ::std::option::Option::Some(v);
    }

    pub fn get_commit<'a>(&self) -> u64 {
        self.commit.unwrap_or(0)
    }
}

impl ::protobuf::Message for HardState {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.term = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.vote = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.commit = ::std::option::Option::Some(tmp);
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.term.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.vote.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.commit.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.term {
            try!(os.write_uint64(1, v));
        };
        if let Some(v) = self.vote {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.commit {
            try!(os.write_uint64(3, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<HardState>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for HardState {
    fn new() -> HardState {
        HardState::new()
    }

    fn descriptor_static(_: ::std::option::Option<HardState>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "term",
                    HardState::has_term,
                    HardState::get_term,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "vote",
                    HardState::has_vote,
                    HardState::get_vote,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "commit",
                    HardState::has_commit,
                    HardState::get_commit,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<HardState>(
                    "HardState",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for HardState {
    fn clear(&mut self) {
        self.clear_term();
        self.clear_vote();
        self.clear_commit();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for HardState {
    fn eq(&self, other: &HardState) -> bool {
        self.term == other.term &&
        self.vote == other.vote &&
        self.commit == other.commit &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for HardState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ConfState {
    // message fields
    nodes: ::std::vec::Vec<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl ConfState {
    pub fn new() -> ConfState {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ConfState {
        static mut instance: ::protobuf::lazy::Lazy<ConfState> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ConfState,
        };
        unsafe {
            instance.get(|| {
                ConfState {
                    nodes: ::std::vec::Vec::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // repeated uint64 nodes = 1;

    pub fn clear_nodes(&mut self) {
        self.nodes.clear();
    }

    // Param is passed by value, moved
    pub fn set_nodes(&mut self, v: ::std::vec::Vec<u64>) {
        self.nodes = v;
    }

    // Mutable pointer to the field.
    pub fn mut_nodes<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u64> {
        &mut self.nodes
    }

    // Take field
    pub fn take_nodes(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.nodes, ::std::vec::Vec::new())
    }

    pub fn get_nodes<'a>(&'a self) -> &'a [u64] {
        &self.nodes
    }
}

impl ::protobuf::Message for ConfState {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_repeated_uint64_into(wire_type, is, &mut self.nodes));
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.nodes.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in self.nodes.iter() {
            try!(os.write_uint64(1, *v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<ConfState>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ConfState {
    fn new() -> ConfState {
        ConfState::new()
    }

    fn descriptor_static(_: ::std::option::Option<ConfState>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_u64_accessor(
                    "nodes",
                    ConfState::get_nodes,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ConfState>(
                    "ConfState",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ConfState {
    fn clear(&mut self) {
        self.clear_nodes();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ConfState {
    fn eq(&self, other: &ConfState) -> bool {
        self.nodes == other.nodes &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ConfState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ConfChange {
    // message fields
    ID: ::std::option::Option<u64>,
    Type: ::std::option::Option<ConfChangeType>,
    NodeID: ::std::option::Option<u64>,
    Context: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl ConfChange {
    pub fn new() -> ConfChange {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ConfChange {
        static mut instance: ::protobuf::lazy::Lazy<ConfChange> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ConfChange,
        };
        unsafe {
            instance.get(|| {
                ConfChange {
                    ID: ::std::option::Option::None,
                    Type: ::std::option::Option::None,
                    NodeID: ::std::option::Option::None,
                    Context: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional uint64 ID = 1;

    pub fn clear_ID(&mut self) {
        self.ID = ::std::option::Option::None;
    }

    pub fn has_ID(&self) -> bool {
        self.ID.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ID(&mut self, v: u64) {
        self.ID = ::std::option::Option::Some(v);
    }

    pub fn get_ID<'a>(&self) -> u64 {
        self.ID.unwrap_or(0)
    }

    // optional .raftpb.ConfChangeType Type = 2;

    pub fn clear_Type(&mut self) {
        self.Type = ::std::option::Option::None;
    }

    pub fn has_Type(&self) -> bool {
        self.Type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_Type(&mut self, v: ConfChangeType) {
        self.Type = ::std::option::Option::Some(v);
    }

    pub fn get_Type<'a>(&self) -> ConfChangeType {
        self.Type.unwrap_or(ConfChangeType::ConfChangeAddNode)
    }

    // optional uint64 NodeID = 3;

    pub fn clear_NodeID(&mut self) {
        self.NodeID = ::std::option::Option::None;
    }

    pub fn has_NodeID(&self) -> bool {
        self.NodeID.is_some()
    }

    // Param is passed by value, moved
    pub fn set_NodeID(&mut self, v: u64) {
        self.NodeID = ::std::option::Option::Some(v);
    }

    pub fn get_NodeID<'a>(&self) -> u64 {
        self.NodeID.unwrap_or(0)
    }

    // optional bytes Context = 4;

    pub fn clear_Context(&mut self) {
        self.Context.clear();
    }

    pub fn has_Context(&self) -> bool {
        self.Context.is_some()
    }

    // Param is passed by value, moved
    pub fn set_Context(&mut self, v: ::std::vec::Vec<u8>) {
        self.Context = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_Context<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.Context.is_none() {
            self.Context.set_default();
        };
        self.Context.as_mut().unwrap()
    }

    // Take field
    pub fn take_Context(&mut self) -> ::std::vec::Vec<u8> {
        self.Context.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_Context<'a>(&'a self) -> &'a [u8] {
        match self.Context.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for ConfChange {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.ID = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_enum());
                    self.Type = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.NodeID = ::std::option::Option::Some(tmp);
                },
                4 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.Context));
                },
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.ID.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.Type.iter() {
            my_size += ::protobuf::rt::enum_size(2, *value);
        };
        for value in self.NodeID.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.Context.iter() {
            my_size += ::protobuf::rt::bytes_size(4, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.ID {
            try!(os.write_uint64(1, v));
        };
        if let Some(v) = self.Type {
            try!(os.write_enum(2, v as i32));
        };
        if let Some(v) = self.NodeID {
            try!(os.write_uint64(3, v));
        };
        if let Some(v) = self.Context.as_ref() {
            try!(os.write_bytes(4, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<ConfChange>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ConfChange {
    fn new() -> ConfChange {
        ConfChange::new()
    }

    fn descriptor_static(_: ::std::option::Option<ConfChange>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "ID",
                    ConfChange::has_ID,
                    ConfChange::get_ID,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "Type",
                    ConfChange::has_Type,
                    ConfChange::get_Type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "NodeID",
                    ConfChange::has_NodeID,
                    ConfChange::get_NodeID,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "Context",
                    ConfChange::has_Context,
                    ConfChange::get_Context,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ConfChange>(
                    "ConfChange",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ConfChange {
    fn clear(&mut self) {
        self.clear_ID();
        self.clear_Type();
        self.clear_NodeID();
        self.clear_Context();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ConfChange {
    fn eq(&self, other: &ConfChange) -> bool {
        self.ID == other.ID &&
        self.Type == other.Type &&
        self.NodeID == other.NodeID &&
        self.Context == other.Context &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ConfChange {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,
}

impl ::protobuf::ProtobufEnum for EntryType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<EntryType> {
        match value {
            0 => ::std::option::Option::Some(EntryType::EntryNormal),
            1 => ::std::option::Option::Some(EntryType::EntryConfChange),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [EntryType] = &[
            EntryType::EntryNormal,
            EntryType::EntryConfChange,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<EntryType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("EntryType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for EntryType {
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum MessageType {
    MsgHup = 0,
    MsgBeat = 1,
    MsgPropose = 2,
    MsgApply = 3,
    MsgApplyResponse = 4,
    MsgVote = 5,
    MsgVoteResponse = 6,
    MsgSnapshot = 7,
    MsgHeartbeat = 8,
    MsgHeartbeatResponse = 9,
    MsgUnreachable = 10,
    MsgSnapStatus = 11,
    MsgCheckQuorum = 12,
}

impl ::protobuf::ProtobufEnum for MessageType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<MessageType> {
        match value {
            0 => ::std::option::Option::Some(MessageType::MsgHup),
            1 => ::std::option::Option::Some(MessageType::MsgBeat),
            2 => ::std::option::Option::Some(MessageType::MsgPropose),
            3 => ::std::option::Option::Some(MessageType::MsgApply),
            4 => ::std::option::Option::Some(MessageType::MsgApplyResponse),
            5 => ::std::option::Option::Some(MessageType::MsgVote),
            6 => ::std::option::Option::Some(MessageType::MsgVoteResponse),
            7 => ::std::option::Option::Some(MessageType::MsgSnapshot),
            8 => ::std::option::Option::Some(MessageType::MsgHeartbeat),
            9 => ::std::option::Option::Some(MessageType::MsgHeartbeatResponse),
            10 => ::std::option::Option::Some(MessageType::MsgUnreachable),
            11 => ::std::option::Option::Some(MessageType::MsgSnapStatus),
            12 => ::std::option::Option::Some(MessageType::MsgCheckQuorum),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [MessageType] = &[
            MessageType::MsgHup,
            MessageType::MsgBeat,
            MessageType::MsgPropose,
            MessageType::MsgApply,
            MessageType::MsgApplyResponse,
            MessageType::MsgVote,
            MessageType::MsgVoteResponse,
            MessageType::MsgSnapshot,
            MessageType::MsgHeartbeat,
            MessageType::MsgHeartbeatResponse,
            MessageType::MsgUnreachable,
            MessageType::MsgSnapStatus,
            MessageType::MsgCheckQuorum,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<MessageType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("MessageType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for MessageType {
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum ConfChangeType {
    ConfChangeAddNode = 0,
    ConfChangeRemoveNode = 1,
    ConfChangeUpdateNode = 2,
}

impl ::protobuf::ProtobufEnum for ConfChangeType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ConfChangeType> {
        match value {
            0 => ::std::option::Option::Some(ConfChangeType::ConfChangeAddNode),
            1 => ::std::option::Option::Some(ConfChangeType::ConfChangeRemoveNode),
            2 => ::std::option::Option::Some(ConfChangeType::ConfChangeUpdateNode),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [ConfChangeType] = &[
            ConfChangeType::ConfChangeAddNode,
            ConfChangeType::ConfChangeRemoveNode,
            ConfChangeType::ConfChangeUpdateNode,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<ConfChangeType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("ConfChangeType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for ConfChangeType {
}

static file_descriptor_proto_data: &'static [u8] = &[
    0x0a, 0x0a, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x72, 0x61,
    0x66, 0x74, 0x70, 0x62, 0x22, 0x53, 0x0a, 0x05, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x1f, 0x0a,
    0x04, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x11, 0x2e, 0x72, 0x61,
    0x66, 0x74, 0x70, 0x62, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0c,
    0x0a, 0x04, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x12, 0x0d, 0x0a, 0x05,
    0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x12, 0x0c, 0x0a, 0x04, 0x44,
    0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x22, 0x56, 0x0a, 0x10, 0x53, 0x6e, 0x61,
    0x70, 0x73, 0x68, 0x6f, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x25, 0x0a,
    0x0a, 0x63, 0x6f, 0x6e, 0x66, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x11, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x53,
    0x74, 0x61, 0x74, 0x65, 0x12, 0x0d, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x02, 0x20,
    0x01, 0x28, 0x04, 0x12, 0x0c, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28,
    0x04, 0x22, 0x44, 0x0a, 0x08, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x12, 0x0c, 0x0a,
    0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x12, 0x2a, 0x0a, 0x08, 0x6d,
    0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e,
    0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x4d,
    0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x22, 0xec, 0x01, 0x0a, 0x07, 0x4d, 0x65, 0x73, 0x73,
    0x61, 0x67, 0x65, 0x12, 0x21, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0e, 0x32, 0x13, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61,
    0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0a, 0x0a, 0x02, 0x74, 0x6f, 0x18, 0x02, 0x20, 0x01,
    0x28, 0x04, 0x12, 0x0c, 0x0a, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04,
    0x12, 0x0c, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x12, 0x0f,
    0x0a, 0x07, 0x6c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x12,
    0x0d, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x06, 0x20, 0x01, 0x28, 0x04, 0x12, 0x1e,
    0x0a, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b, 0x32,
    0x0d, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x0e,
    0x0a, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28, 0x04, 0x12, 0x22,
    0x0a, 0x08, 0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b,
    0x32, 0x10, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68,
    0x6f, 0x74, 0x12, 0x0e, 0x0a, 0x06, 0x72, 0x65, 0x6a, 0x65, 0x63, 0x74, 0x18, 0x0a, 0x20, 0x01,
    0x28, 0x08, 0x12, 0x12, 0x0a, 0x0a, 0x72, 0x65, 0x6a, 0x65, 0x63, 0x74, 0x48, 0x69, 0x6e, 0x74,
    0x18, 0x0b, 0x20, 0x01, 0x28, 0x04, 0x22, 0x37, 0x0a, 0x09, 0x48, 0x61, 0x72, 0x64, 0x53, 0x74,
    0x61, 0x74, 0x65, 0x12, 0x0c, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x04, 0x12, 0x0c, 0x0a, 0x04, 0x76, 0x6f, 0x74, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x12,
    0x0e, 0x0a, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x22,
    0x1a, 0x0a, 0x09, 0x43, 0x6f, 0x6e, 0x66, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x0d, 0x0a, 0x05,
    0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x04, 0x22, 0x5f, 0x0a, 0x0a, 0x43,
    0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x0a, 0x0a, 0x02, 0x49, 0x44, 0x18,
    0x01, 0x20, 0x01, 0x28, 0x04, 0x12, 0x24, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20,
    0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6e,
    0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0e, 0x0a, 0x06, 0x4e,
    0x6f, 0x64, 0x65, 0x49, 0x44, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x12, 0x0f, 0x0a, 0x07, 0x43,
    0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x2a, 0x31, 0x0a, 0x09,
    0x45, 0x6e, 0x74, 0x72, 0x79, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0f, 0x0a, 0x0b, 0x45, 0x6e, 0x74,
    0x72, 0x79, 0x4e, 0x6f, 0x72, 0x6d, 0x61, 0x6c, 0x10, 0x00, 0x12, 0x13, 0x0a, 0x0f, 0x45, 0x6e,
    0x74, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x10, 0x01, 0x2a,
    0xf4, 0x01, 0x0a, 0x0b, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12,
    0x0a, 0x0a, 0x06, 0x4d, 0x73, 0x67, 0x48, 0x75, 0x70, 0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x4d,
    0x73, 0x67, 0x42, 0x65, 0x61, 0x74, 0x10, 0x01, 0x12, 0x0e, 0x0a, 0x0a, 0x4d, 0x73, 0x67, 0x50,
    0x72, 0x6f, 0x70, 0x6f, 0x73, 0x65, 0x10, 0x02, 0x12, 0x0c, 0x0a, 0x08, 0x4d, 0x73, 0x67, 0x41,
    0x70, 0x70, 0x6c, 0x79, 0x10, 0x03, 0x12, 0x14, 0x0a, 0x10, 0x4d, 0x73, 0x67, 0x41, 0x70, 0x70,
    0x6c, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x10, 0x04, 0x12, 0x0b, 0x0a, 0x07,
    0x4d, 0x73, 0x67, 0x56, 0x6f, 0x74, 0x65, 0x10, 0x05, 0x12, 0x13, 0x0a, 0x0f, 0x4d, 0x73, 0x67,
    0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x10, 0x06, 0x12, 0x0f,
    0x0a, 0x0b, 0x4d, 0x73, 0x67, 0x53, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x10, 0x07, 0x12,
    0x10, 0x0a, 0x0c, 0x4d, 0x73, 0x67, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x10,
    0x08, 0x12, 0x18, 0x0a, 0x14, 0x4d, 0x73, 0x67, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61,
    0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x10, 0x09, 0x12, 0x12, 0x0a, 0x0e, 0x4d,
    0x73, 0x67, 0x55, 0x6e, 0x72, 0x65, 0x61, 0x63, 0x68, 0x61, 0x62, 0x6c, 0x65, 0x10, 0x0a, 0x12,
    0x11, 0x0a, 0x0d, 0x4d, 0x73, 0x67, 0x53, 0x6e, 0x61, 0x70, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73,
    0x10, 0x0b, 0x12, 0x12, 0x0a, 0x0e, 0x4d, 0x73, 0x67, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x51, 0x75,
    0x6f, 0x72, 0x75, 0x6d, 0x10, 0x0c, 0x2a, 0x5b, 0x0a, 0x0e, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68,
    0x61, 0x6e, 0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x15, 0x0a, 0x11, 0x43, 0x6f, 0x6e, 0x66,
    0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x41, 0x64, 0x64, 0x4e, 0x6f, 0x64, 0x65, 0x10, 0x00, 0x12,
    0x18, 0x0a, 0x14, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x6d,
    0x6f, 0x76, 0x65, 0x4e, 0x6f, 0x64, 0x65, 0x10, 0x01, 0x12, 0x18, 0x0a, 0x14, 0x43, 0x6f, 0x6e,
    0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4e, 0x6f, 0x64,
    0x65, 0x10, 0x02, 0x4a, 0xf0, 0x16, 0x0a, 0x06, 0x12, 0x04, 0x00, 0x00, 0x4e, 0x01, 0x0a, 0x08,
    0x0a, 0x01, 0x02, 0x12, 0x03, 0x01, 0x08, 0x0e, 0x0a, 0x0a, 0x0a, 0x02, 0x05, 0x00, 0x12, 0x04,
    0x04, 0x00, 0x07, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x00, 0x01, 0x12, 0x03, 0x04, 0x05, 0x0e,
    0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x00, 0x12, 0x03, 0x05, 0x08, 0x1c, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x00, 0x02, 0x00, 0x01, 0x12, 0x03, 0x05, 0x08, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x00, 0x02, 0x00, 0x02, 0x12, 0x03, 0x05, 0x1a, 0x1b, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02,
    0x01, 0x12, 0x03, 0x06, 0x08, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x01, 0x12,
    0x03, 0x06, 0x08, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x02, 0x12, 0x03, 0x06,
    0x1a, 0x1b, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x00, 0x12, 0x04, 0x09, 0x00, 0x0e, 0x01, 0x0a, 0x0a,
    0x0a, 0x03, 0x04, 0x00, 0x01, 0x12, 0x03, 0x09, 0x08, 0x0d, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00,
    0x02, 0x00, 0x12, 0x03, 0x0a, 0x08, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x04,
    0x12, 0x03, 0x0a, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x06, 0x12, 0x03,
    0x0a, 0x11, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x01, 0x12, 0x03, 0x0a, 0x1c,
    0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x03, 0x12, 0x03, 0x0a, 0x24, 0x25, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x01, 0x12, 0x03, 0x0b, 0x08, 0x26, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x00, 0x02, 0x01, 0x04, 0x12, 0x03, 0x0b, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
    0x02, 0x01, 0x05, 0x12, 0x03, 0x0b, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01,
    0x01, 0x12, 0x03, 0x0b, 0x1c, 0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x03, 0x12,
    0x03, 0x0b, 0x24, 0x25, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x02, 0x12, 0x03, 0x0c, 0x08,
    0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x02, 0x04, 0x12, 0x03, 0x0c, 0x08, 0x10, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x02, 0x05, 0x12, 0x03, 0x0c, 0x11, 0x17, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x00, 0x02, 0x02, 0x01, 0x12, 0x03, 0x0c, 0x1c, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x00, 0x02, 0x02, 0x03, 0x12, 0x03, 0x0c, 0x24, 0x25, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02,
    0x03, 0x12, 0x03, 0x0d, 0x08, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x03, 0x04, 0x12,
    0x03, 0x0d, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x03, 0x05, 0x12, 0x03, 0x0d,
    0x11, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x03, 0x01, 0x12, 0x03, 0x0d, 0x1c, 0x20,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x03, 0x03, 0x12, 0x03, 0x0d, 0x24, 0x25, 0x0a, 0x0a,
    0x0a, 0x02, 0x04, 0x01, 0x12, 0x04, 0x10, 0x00, 0x14, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x01,
    0x01, 0x12, 0x03, 0x10, 0x08, 0x18, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x01, 0x02, 0x00, 0x12, 0x03,
    0x11, 0x08, 0x2a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x04, 0x12, 0x03, 0x11, 0x08,
    0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x06, 0x12, 0x03, 0x11, 0x11, 0x1a, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x01, 0x12, 0x03, 0x11, 0x1b, 0x25, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x01, 0x02, 0x00, 0x03, 0x12, 0x03, 0x11, 0x28, 0x29, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x01, 0x02, 0x01, 0x12, 0x03, 0x12, 0x08, 0x2a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x01,
    0x04, 0x12, 0x03, 0x12, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x01, 0x05, 0x12,
    0x03, 0x12, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x01, 0x01, 0x12, 0x03, 0x12,
    0x1b, 0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x01, 0x03, 0x12, 0x03, 0x12, 0x28, 0x29,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x01, 0x02, 0x02, 0x12, 0x03, 0x13, 0x08, 0x2a, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x01, 0x02, 0x02, 0x04, 0x12, 0x03, 0x13, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x01, 0x02, 0x02, 0x05, 0x12, 0x03, 0x13, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02,
    0x02, 0x01, 0x12, 0x03, 0x13, 0x1b, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x02, 0x03,
    0x12, 0x03, 0x13, 0x28, 0x29, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x02, 0x12, 0x04, 0x16, 0x00, 0x19,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x02, 0x01, 0x12, 0x03, 0x16, 0x08, 0x10, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x02, 0x02, 0x00, 0x12, 0x03, 0x17, 0x08, 0x2f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
    0x02, 0x00, 0x04, 0x12, 0x03, 0x17, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00,
    0x05, 0x12, 0x03, 0x17, 0x11, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x01, 0x12,
    0x03, 0x17, 0x22, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x03, 0x12, 0x03, 0x17,
    0x2d, 0x2e, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02, 0x01, 0x12, 0x03, 0x18, 0x08, 0x2f, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x04, 0x12, 0x03, 0x18, 0x08, 0x10, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x02, 0x02, 0x01, 0x06, 0x12, 0x03, 0x18, 0x11, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x02, 0x02, 0x01, 0x01, 0x12, 0x03, 0x18, 0x22, 0x2a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x18, 0x2d, 0x2e, 0x0a, 0x0a, 0x0a, 0x02, 0x05, 0x01, 0x12, 0x04, 0x1b,
    0x00, 0x29, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x01, 0x01, 0x12, 0x03, 0x1b, 0x05, 0x10, 0x0a,
    0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x00, 0x12, 0x03, 0x1c, 0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05,
    0x05, 0x01, 0x02, 0x00, 0x01, 0x12, 0x03, 0x1c, 0x08, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01,
    0x02, 0x00, 0x02, 0x12, 0x03, 0x1c, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x01,
    0x12, 0x03, 0x1d, 0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x01, 0x01, 0x12, 0x03,
    0x1d, 0x08, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x01, 0x02, 0x12, 0x03, 0x1d, 0x22,
    0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x02, 0x12, 0x03, 0x1e, 0x08, 0x24, 0x0a, 0x0c,
    0x0a, 0x05, 0x05, 0x01, 0x02, 0x02, 0x01, 0x12, 0x03, 0x1e, 0x08, 0x12, 0x0a, 0x0c, 0x0a, 0x05,
    0x05, 0x01, 0x02, 0x02, 0x02, 0x12, 0x03, 0x1e, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01,
    0x02, 0x03, 0x12, 0x03, 0x1f, 0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x03, 0x01,
    0x12, 0x03, 0x1f, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x03, 0x02, 0x12, 0x03,
    0x1f, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x04, 0x12, 0x03, 0x20, 0x08, 0x24,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x04, 0x01, 0x12, 0x03, 0x20, 0x08, 0x18, 0x0a, 0x0c,
    0x0a, 0x05, 0x05, 0x01, 0x02, 0x04, 0x02, 0x12, 0x03, 0x20, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04,
    0x05, 0x01, 0x02, 0x05, 0x12, 0x03, 0x21, 0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02,
    0x05, 0x01, 0x12, 0x03, 0x21, 0x08, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x05, 0x02,
    0x12, 0x03, 0x21, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x06, 0x12, 0x03, 0x22,
    0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x06, 0x01, 0x12, 0x03, 0x22, 0x08, 0x17,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x06, 0x02, 0x12, 0x03, 0x22, 0x22, 0x23, 0x0a, 0x0b,
    0x0a, 0x04, 0x05, 0x01, 0x02, 0x07, 0x12, 0x03, 0x23, 0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x01, 0x02, 0x07, 0x01, 0x12, 0x03, 0x23, 0x08, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02,
    0x07, 0x02, 0x12, 0x03, 0x23, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x08, 0x12,
    0x03, 0x24, 0x08, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x08, 0x01, 0x12, 0x03, 0x24,
    0x08, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x08, 0x02, 0x12, 0x03, 0x24, 0x22, 0x23,
    0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x09, 0x12, 0x03, 0x25, 0x08, 0x24, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x01, 0x02, 0x09, 0x01, 0x12, 0x03, 0x25, 0x08, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x01, 0x02, 0x09, 0x02, 0x12, 0x03, 0x25, 0x22, 0x23, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02,
    0x0a, 0x12, 0x03, 0x26, 0x08, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x0a, 0x01, 0x12,
    0x03, 0x26, 0x08, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x0a, 0x02, 0x12, 0x03, 0x26,
    0x22, 0x24, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x0b, 0x12, 0x03, 0x27, 0x08, 0x25, 0x0a,
    0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x0b, 0x01, 0x12, 0x03, 0x27, 0x08, 0x15, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x01, 0x02, 0x0b, 0x02, 0x12, 0x03, 0x27, 0x22, 0x24, 0x0a, 0x0b, 0x0a, 0x04, 0x05,
    0x01, 0x02, 0x0c, 0x12, 0x03, 0x28, 0x08, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x0c,
    0x01, 0x12, 0x03, 0x28, 0x08, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x0c, 0x02, 0x12,
    0x03, 0x28, 0x22, 0x24, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x03, 0x12, 0x04, 0x2b, 0x00, 0x37, 0x01,
    0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x03, 0x01, 0x12, 0x03, 0x2b, 0x08, 0x0f, 0x0a, 0x0b, 0x0a, 0x04,
    0x04, 0x03, 0x02, 0x00, 0x12, 0x03, 0x2c, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02,
    0x00, 0x04, 0x12, 0x03, 0x2c, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x06,
    0x12, 0x03, 0x2c, 0x11, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x01, 0x12, 0x03,
    0x2c, 0x1d, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x03, 0x12, 0x03, 0x2c, 0x2b,
    0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x01, 0x12, 0x03, 0x2d, 0x08, 0x2d, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x03, 0x02, 0x01, 0x04, 0x12, 0x03, 0x2d, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x03, 0x02, 0x01, 0x05, 0x12, 0x03, 0x2d, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03,
    0x02, 0x01, 0x01, 0x12, 0x03, 0x2d, 0x1d, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x01,
    0x03, 0x12, 0x03, 0x2d, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x02, 0x12, 0x03,
    0x2e, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x02, 0x04, 0x12, 0x03, 0x2e, 0x08,
    0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x02, 0x05, 0x12, 0x03, 0x2e, 0x11, 0x17, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x02, 0x01, 0x12, 0x03, 0x2e, 0x1d, 0x21, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x03, 0x02, 0x02, 0x03, 0x12, 0x03, 0x2e, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x03, 0x02, 0x03, 0x12, 0x03, 0x2f, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x03,
    0x04, 0x12, 0x03, 0x2f, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x03, 0x05, 0x12,
    0x03, 0x2f, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x03, 0x01, 0x12, 0x03, 0x2f,
    0x1d, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x03, 0x03, 0x12, 0x03, 0x2f, 0x2b, 0x2c,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x04, 0x12, 0x03, 0x30, 0x08, 0x2d, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x03, 0x02, 0x04, 0x04, 0x12, 0x03, 0x30, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x03, 0x02, 0x04, 0x05, 0x12, 0x03, 0x30, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02,
    0x04, 0x01, 0x12, 0x03, 0x30, 0x1d, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x04, 0x03,
    0x12, 0x03, 0x30, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x05, 0x12, 0x03, 0x31,
    0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x05, 0x04, 0x12, 0x03, 0x31, 0x08, 0x10,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x05, 0x05, 0x12, 0x03, 0x31, 0x11, 0x17, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x03, 0x02, 0x05, 0x01, 0x12, 0x03, 0x31, 0x1d, 0x22, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x03, 0x02, 0x05, 0x03, 0x12, 0x03, 0x31, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03,
    0x02, 0x06, 0x12, 0x03, 0x32, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x06, 0x04,
    0x12, 0x03, 0x32, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x06, 0x06, 0x12, 0x03,
    0x32, 0x11, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x06, 0x01, 0x12, 0x03, 0x32, 0x1d,
    0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x06, 0x03, 0x12, 0x03, 0x32, 0x2b, 0x2c, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x07, 0x12, 0x03, 0x33, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x03, 0x02, 0x07, 0x04, 0x12, 0x03, 0x33, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03,
    0x02, 0x07, 0x05, 0x12, 0x03, 0x33, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x07,
    0x01, 0x12, 0x03, 0x33, 0x1d, 0x23, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x07, 0x03, 0x12,
    0x03, 0x33, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x08, 0x12, 0x03, 0x34, 0x08,
    0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x08, 0x04, 0x12, 0x03, 0x34, 0x08, 0x10, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x08, 0x06, 0x12, 0x03, 0x34, 0x11, 0x19, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x03, 0x02, 0x08, 0x01, 0x12, 0x03, 0x34, 0x1d, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x03, 0x02, 0x08, 0x03, 0x12, 0x03, 0x34, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02,
    0x09, 0x12, 0x03, 0x35, 0x08, 0x2e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x09, 0x04, 0x12,
    0x03, 0x35, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x09, 0x05, 0x12, 0x03, 0x35,
    0x11, 0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x09, 0x01, 0x12, 0x03, 0x35, 0x1d, 0x23,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x09, 0x03, 0x12, 0x03, 0x35, 0x2b, 0x2d, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x03, 0x02, 0x0a, 0x12, 0x03, 0x36, 0x08, 0x2e, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x03, 0x02, 0x0a, 0x04, 0x12, 0x03, 0x36, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02,
    0x0a, 0x05, 0x12, 0x03, 0x36, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x0a, 0x01,
    0x12, 0x03, 0x36, 0x1d, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x0a, 0x03, 0x12, 0x03,
    0x36, 0x2b, 0x2d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x04, 0x12, 0x04, 0x39, 0x00, 0x3d, 0x01, 0x0a,
    0x0a, 0x0a, 0x03, 0x04, 0x04, 0x01, 0x12, 0x03, 0x39, 0x08, 0x11, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x04, 0x02, 0x00, 0x12, 0x03, 0x3a, 0x08, 0x23, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00,
    0x04, 0x12, 0x03, 0x3a, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x05, 0x12,
    0x03, 0x3a, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x01, 0x12, 0x03, 0x3a,
    0x18, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x03, 0x12, 0x03, 0x3a, 0x21, 0x22,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x01, 0x12, 0x03, 0x3b, 0x08, 0x23, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x04, 0x02, 0x01, 0x04, 0x12, 0x03, 0x3b, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x04, 0x02, 0x01, 0x05, 0x12, 0x03, 0x3b, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02,
    0x01, 0x01, 0x12, 0x03, 0x3b, 0x18, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01, 0x03,
    0x12, 0x03, 0x3b, 0x21, 0x22, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x02, 0x12, 0x03, 0x3c,
    0x08, 0x23, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x04, 0x12, 0x03, 0x3c, 0x08, 0x10,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x05, 0x12, 0x03, 0x3c, 0x11, 0x17, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x01, 0x12, 0x03, 0x3c, 0x18, 0x1e, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x04, 0x02, 0x02, 0x03, 0x12, 0x03, 0x3c, 0x21, 0x22, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x05,
    0x12, 0x04, 0x3f, 0x00, 0x41, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x05, 0x01, 0x12, 0x03, 0x3f,
    0x08, 0x11, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x00, 0x12, 0x03, 0x40, 0x08, 0x22, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x00, 0x04, 0x12, 0x03, 0x40, 0x08, 0x10, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x05, 0x02, 0x00, 0x05, 0x12, 0x03, 0x40, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x05, 0x02, 0x00, 0x01, 0x12, 0x03, 0x40, 0x18, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02,
    0x00, 0x03, 0x12, 0x03, 0x40, 0x20, 0x21, 0x0a, 0x0a, 0x0a, 0x02, 0x05, 0x02, 0x12, 0x04, 0x43,
    0x00, 0x47, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x02, 0x01, 0x12, 0x03, 0x43, 0x05, 0x13, 0x0a,
    0x0b, 0x0a, 0x04, 0x05, 0x02, 0x02, 0x00, 0x12, 0x03, 0x44, 0x08, 0x21, 0x0a, 0x0c, 0x0a, 0x05,
    0x05, 0x02, 0x02, 0x00, 0x01, 0x12, 0x03, 0x44, 0x08, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x02,
    0x02, 0x00, 0x02, 0x12, 0x03, 0x44, 0x1f, 0x20, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x02, 0x02, 0x01,
    0x12, 0x03, 0x45, 0x08, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x02, 0x02, 0x01, 0x01, 0x12, 0x03,
    0x45, 0x08, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x02, 0x02, 0x01, 0x02, 0x12, 0x03, 0x45, 0x1f,
    0x20, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x02, 0x02, 0x02, 0x12, 0x03, 0x46, 0x08, 0x21, 0x0a, 0x0c,
    0x0a, 0x05, 0x05, 0x02, 0x02, 0x02, 0x01, 0x12, 0x03, 0x46, 0x08, 0x1c, 0x0a, 0x0c, 0x0a, 0x05,
    0x05, 0x02, 0x02, 0x02, 0x02, 0x12, 0x03, 0x46, 0x1f, 0x20, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x06,
    0x12, 0x04, 0x49, 0x00, 0x4e, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x06, 0x01, 0x12, 0x03, 0x49,
    0x08, 0x12, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x06, 0x02, 0x00, 0x12, 0x03, 0x4a, 0x08, 0x2d, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x00, 0x04, 0x12, 0x03, 0x4a, 0x08, 0x10, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x06, 0x02, 0x00, 0x05, 0x12, 0x03, 0x4a, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x06, 0x02, 0x00, 0x01, 0x12, 0x03, 0x4a, 0x21, 0x23, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02,
    0x00, 0x03, 0x12, 0x03, 0x4a, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x06, 0x02, 0x01, 0x12,
    0x03, 0x4b, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x01, 0x04, 0x12, 0x03, 0x4b,
    0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x01, 0x06, 0x12, 0x03, 0x4b, 0x11, 0x1f,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x01, 0x01, 0x12, 0x03, 0x4b, 0x21, 0x25, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x06, 0x02, 0x01, 0x03, 0x12, 0x03, 0x4b, 0x2b, 0x2c, 0x0a, 0x0b, 0x0a, 0x04,
    0x04, 0x06, 0x02, 0x02, 0x12, 0x03, 0x4c, 0x08, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02,
    0x02, 0x04, 0x12, 0x03, 0x4c, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x02, 0x05,
    0x12, 0x03, 0x4c, 0x11, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x02, 0x01, 0x12, 0x03,
    0x4c, 0x21, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x02, 0x03, 0x12, 0x03, 0x4c, 0x2b,
    0x2c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x06, 0x02, 0x03, 0x12, 0x03, 0x4d, 0x08, 0x2d, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x06, 0x02, 0x03, 0x04, 0x12, 0x03, 0x4d, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x06, 0x02, 0x03, 0x05, 0x12, 0x03, 0x4d, 0x11, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06,
    0x02, 0x03, 0x01, 0x12, 0x03, 0x4d, 0x21, 0x28, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x03,
    0x03, 0x12, 0x03, 0x4d, 0x2b, 0x2c,
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
