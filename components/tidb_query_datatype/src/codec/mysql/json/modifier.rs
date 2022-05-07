// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ptr;

use codec::number::NumberCodec;

use super::{
    super::Result,
    constants::*,
    json_extract::extract_json,
    path_expr::{PathExpression, PathLeg},
    Json, JsonRef, JsonType,
};

/// A helper struct that derives a new JSON by combining and manipulating
/// the encoded bytes directly. Only used by `json_replace`, `json_set`,
/// `json_insert` and `json_remove`
///
/// See `binaryModifier` in TiDB `json/binary_function.go`
pub struct BinaryModifier<'a> {
    // The target Json to be modified
    old: JsonRef<'a>,
    // The ptr point to the memory location of `old.value` that `new_value` should be appended
    to_be_modified_ptr: *const u8,
    // The new encoded value
    // TODO(fullstop000): Can we just use Json instead ?
    new_value: Option<Json>,
}

impl<'a> BinaryModifier<'a> {
    /// Creates a new `BinaryModifier` from a `JsonRef`
    pub fn new(old: JsonRef<'a>) -> BinaryModifier<'_> {
        Self {
            // The initial offset is 0 by `as_ref()` call
            old,
            // Mark invalid
            to_be_modified_ptr: ptr::null(),
            new_value: None,
        }
    }

    /// Replaces the existing value JSON and adds nonexisting value
    /// specified by the expression path with `new`
    pub fn set(mut self, path: &PathExpression, new: Json) -> Result<Json> {
        let result = extract_json(self.old, path.legs.as_slice())?;
        if !result.is_empty() {
            self.to_be_modified_ptr = result[0].as_ptr();
            self.new_value = Some(new);
        } else {
            self.do_insert(&path.legs, new)?;
        }
        self.rebuild()
    }

    /// Replaces the existing value JSON specified by the expression path with `new`
    pub fn replace(mut self, path: &PathExpression, new: Json) -> Result<Json> {
        let result = extract_json(self.old, path.legs.as_slice())?;
        if result.is_empty() {
            return Ok(self.old.to_owned());
        }
        self.to_be_modified_ptr = result[0].as_ptr();
        self.new_value = Some(new);
        self.rebuild()
    }

    /// Inserts a `new` into `old` JSON document by given expression path without replacing
    /// existing values
    pub fn insert(mut self, path: &PathExpression, new: Json) -> Result<Json> {
        let result = extract_json(self.old, path.legs.as_slice())?;
        if !result.is_empty() {
            // The path-value is existing. The insertion is ignored with no overwrite.
            return Ok(self.old.to_owned());
        }
        self.do_insert(path.legs.as_slice(), new)?;
        self.rebuild()
    }

    fn do_insert(&mut self, path_legs: &[PathLeg], new: Json) -> Result<()> {
        if path_legs.is_empty() {
            return Ok(());
        }
        let legs_len = path_legs.len();
        let (parent_legs, last_leg) = (&path_legs[..legs_len - 1], &path_legs[legs_len - 1]);
        let result = extract_json(self.old, parent_legs)?;
        if result.is_empty() {
            return Ok(());
        }
        let parent_node = &result[0];
        match &*last_leg {
            PathLeg::Index(_) => {
                // Record the parent node value offset, as it's actually relative to `old`
                self.to_be_modified_ptr = parent_node.as_ptr();
                match parent_node.get_type() {
                    JsonType::Array => {
                        let elem_count = parent_node.get_elem_count();
                        let mut elems = Vec::with_capacity(elem_count + 1);
                        for i in 0..elem_count {
                            elems.push(parent_node.array_get_elem(i)?);
                        }
                        // We can ignore the idx in the PathLeg here since we have checked the path-value existence
                        elems.push(new.as_ref());
                        self.new_value = Some(Json::from_ref_array(elems)?);
                    }
                    _ => {
                        let new_value = vec![*parent_node, new.as_ref()];
                        self.new_value = Some(Json::from_ref_array(new_value)?);
                    }
                }
            }
            PathLeg::Key(insert_key) => {
                // Ignore constant
                if parent_node.get_type() != JsonType::Object {
                    return Ok(());
                }
                self.to_be_modified_ptr = parent_node.as_ptr();
                let elem_count = parent_node.get_elem_count();
                let mut entries = Vec::with_capacity(elem_count + 1);
                match parent_node.object_search_key(insert_key.as_bytes()) {
                    Some(insert_idx) => {
                        for i in 0..elem_count {
                            if insert_idx == i {
                                entries.push((insert_key.as_bytes(), new.as_ref()));
                            }
                            entries.push((
                                parent_node.object_get_key(i),
                                parent_node.object_get_val(i)?,
                            ));
                        }
                    }
                    None => {
                        for i in 0..elem_count {
                            entries.push((
                                parent_node.object_get_key(i),
                                parent_node.object_get_val(i)?,
                            ));
                        }
                        entries.push((insert_key.as_bytes(), new.as_ref()))
                    }
                }
                self.new_value = Some(Json::from_kv_pairs(entries)?);
            }
            _ => {}
        }
        Ok(())
    }

    pub fn remove(mut self, path_legs: &[PathLeg]) -> Result<Json> {
        let result = extract_json(self.old, path_legs)?;
        if result.is_empty() {
            return Ok(self.old.to_owned());
        }
        self.do_remove(path_legs)?;
        self.rebuild()
    }

    fn do_remove(&mut self, path_legs: &[PathLeg]) -> Result<()> {
        if path_legs.is_empty() {
            return Ok(());
        }
        let legs_len = path_legs.len();
        let (parent_legs, last_leg) = (&path_legs[..legs_len - 1], &path_legs[legs_len - 1]);
        let result = extract_json(self.old, parent_legs)?;
        if result.is_empty() {
            // No parent found, just return
            return Ok(());
        }
        let parent_node = &result[0];
        match &*last_leg {
            PathLeg::Index(remove_idx) => {
                if parent_node.get_type() == JsonType::Array {
                    self.to_be_modified_ptr = parent_node.as_ptr();
                    let elems_count = parent_node.get_elem_count();
                    let mut elems = Vec::with_capacity(elems_count - 1);
                    let remove_idx = *remove_idx as usize;
                    for i in 0..elems_count {
                        if i != remove_idx {
                            elems.push(parent_node.array_get_elem(i)?);
                        }
                    }
                    self.new_value = Some(Json::from_ref_array(elems)?);
                }
            }
            PathLeg::Key(remove_key) => {
                // Ignore constant
                if parent_node.get_type() == JsonType::Object {
                    self.to_be_modified_ptr = parent_node.as_ptr();
                    let elem_count = parent_node.get_elem_count();
                    let mut entries = Vec::with_capacity(elem_count - 1);
                    for i in 0..elem_count {
                        let key = parent_node.object_get_key(i);
                        if key != remove_key.as_bytes() {
                            entries.push((key, parent_node.object_get_val(i)?));
                        }
                    }
                    self.new_value = Some(Json::from_kv_pairs(entries)?);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn rebuild(&mut self) -> Result<Json> {
        let mut buf = Vec::with_capacity(
            self.old.value.len() + self.new_value.as_ref().map_or(0, |v| v.value.len()),
        );
        let new_tp = self.rebuild_to(&mut buf)?;
        Ok(Json::new(new_tp, buf))
    }

    // Apply `new_value` as a modification to `old` by encoding the result into
    // the given buffer
    //
    // Returns the old JSON's `JsonType` if the old is untouched or
    // returns the new appended JSON's `JsonType` if the old has been modified
    fn rebuild_to(&mut self, buf: &mut Vec<u8>) -> Result<JsonType> {
        if self.to_be_modified_ptr == self.old.as_ptr() {
            // Replace the old directly
            self.to_be_modified_ptr = ptr::null();
            buf.extend_from_slice(&self.new_value.as_ref().unwrap().value);
            return Ok(self.new_value.as_ref().unwrap().as_ref().get_type());
        } else if self.to_be_modified_ptr.is_null() {
            // No modification, use the old one
            buf.extend_from_slice(self.old.value);
            return Ok(self.old.get_type());
        }
        let tp = self.old.get_type();
        match tp {
            JsonType::Literal
            | JsonType::I64
            | JsonType::U64
            | JsonType::Double
            | JsonType::String => {
                buf.extend_from_slice(self.old.value);
            }
            JsonType::Object | JsonType::Array => {
                let doc_off = buf.len();
                let elem_count = self.old.get_elem_count();
                let current = self.old;
                let val_entry_start = match current.get_type() {
                    JsonType::Array => {
                        let copy_size = HEADER_LEN + elem_count * VALUE_ENTRY_LEN;
                        buf.extend_from_slice(&current.value[..copy_size]);
                        HEADER_LEN
                    }
                    JsonType::Object => {
                        let copy_size = HEADER_LEN + elem_count * (KEY_ENTRY_LEN + VALUE_ENTRY_LEN);
                        // Append kv entries
                        buf.extend_from_slice(&current.value[..copy_size]);
                        // Append keys
                        if elem_count > 0 {
                            let first_key_offset =
                                NumberCodec::decode_u32_le(&current.value[HEADER_LEN..]) as usize;
                            let last_key_offset = NumberCodec::decode_u32_le(
                                &current.value[HEADER_LEN + (elem_count - 1) * KEY_ENTRY_LEN..],
                            ) as usize;
                            let last_key_len = NumberCodec::decode_u16_le(
                                &current.value
                                    [HEADER_LEN + (elem_count - 1) * KEY_ENTRY_LEN + U32_LEN..],
                            ) as usize;
                            buf.extend_from_slice(
                                &current.value[first_key_offset..last_key_offset + last_key_len],
                            );
                        }
                        HEADER_LEN + elem_count * KEY_ENTRY_LEN
                    }
                    // This must be impossible
                    _ => return Err(box_err!("Unexpected source json type")),
                };
                // Resolve values
                for i in 0..elem_count {
                    let val_entry_offset = val_entry_start + i * VALUE_ENTRY_LEN;
                    self.old = current.val_entry_get(val_entry_offset)?;
                    let val_offset = buf.len() - doc_off;
                    // loop until finding the target ptr to be modified
                    let new_tp = self.rebuild_to(buf)?;
                    buf[doc_off + val_entry_offset] = new_tp as u8;
                    match new_tp {
                        JsonType::Literal => {
                            let last_idx = buf.len() - 1;
                            let val = u32::from(buf[last_idx]);
                            NumberCodec::encode_u32_le(
                                &mut buf[doc_off + val_entry_offset + TYPE_LEN..],
                                val,
                            );
                            // TODO: is this necessary?
                            buf.resize(last_idx, 0);
                        }
                        _ => {
                            NumberCodec::encode_u32_le(
                                &mut buf[doc_off + val_entry_offset + TYPE_LEN..],
                                val_offset as u32,
                            );
                        }
                    }
                }
                let data_len = buf.len() - doc_off;
                NumberCodec::encode_u32_le(
                    &mut buf[doc_off + ELEMENT_COUNT_LEN..],
                    data_len as u32,
                );
            }
        }
        Ok(tp)
    }
}
