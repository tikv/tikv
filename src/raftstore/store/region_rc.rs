// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/*!

Manage region reference counts. A region can be referenced by snapshots.

If a region is referenced by some snapshots, when it is transferred away,
its data must NOT be reclaimed via `RemoveFilesInRange` to avoid breaking
the snapshot.

Initially, all regions do not have references. The region will be
referenced when a snapshot is taken. The region will be de-referenced if
the snapshot is dropped.

When a region splits, its references should be copied to the new region
because the new region is a subset of the old region and the snapshot's
range is not changed:

```
Before split:           After split A into A and B:

[   Region A   ]        [ Region A ][ Region B ]
 ^^^^^^^^^^^^^^          ^^^^^^^^^^^^^^^^^^^^^^
   snapshot S                  snapshot S
```

When two regions merge, their references should be merged as well:

```
Before merge:             After merge A and B:

[ A ][ B ][ C ]           [       Region D       ][ Region C ]
 ^^^  ^^^  ^^^              ^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^
  1    2    3                       1, 2               3
 ^^^^^^^^^^^^^              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
       4                                    4, 5
      ^^^^^^^^
          5
```

## Algorithm

To achieve high performance, we are not actually maintaining all
references. Instead, multiple references are hold by one single
atomic variable instance.

Here we use `R1 <- A1(S1, S2)` to denote that region `R1` has two
snapshot references (from `S1` and `S2`) and both of them are hold
by a single atomic instance `A1`.

### Scenario 1. R1 ref by S1, S2, S3, S4

These four references are maintained by the same atomic instance `A1`:

```
R1 <- A1(S1, S2, S3, S4)
```

### Scenario 2. R1 split into R1 and R2

Since references should be copied to the new region, `R2` shares the
same atomic instance `A1`:

```
R1 <- A1(S1, S2, S3, S4)
R2 <- A1(S1, S2, S3, S4)
```

### Scenario 2. R1 ref by S5, S6

`S5` and `S6` reference `R1` only, and should not reference `R2` so that we
need a new atomic variable `A2`:

```
R1 <- A1(S1, S2, S3, S4), A2(S5, S6)
R2 <- A1(S1, S2, S3, S4)
```

It can be seen that, if an atomic variable (`A1` in this case) is shared
for multiple regions (`R1` and `R2`), it can not hold new references (`S5`
and `S6`) because new reference always refer to a single region (`R1`):
`R2` should not be affected.

### Scenario 3. R2 ref by S7

For the same reason in scenario 2, we need a new atomic variable `A3` to
hold reference `S7` because this reference is not shared for multiple
regions:

```
R1 <- A1(S1, S2, S3, S4), A2(S5, S6)
R2 <- A1(S1, S2, S3, S4), A3(S7)
```

### Scenario 4. S1 drop

Previously `S1` reference `R1` and after split it reference `R1` and `R2`
both. When `S1` is dropped, it no longer reference them:

```
R1 <- A1(S2, S3, S4), A2(S5, S6)
R2 <- A1(S2, S3, S4), A3(S7)
```

### Scenario 5. S6 drop

```
R1 <- A1(S2, S3, S4), A2(S5)
R2 <- A1(S2, S3, S4), A3(S7)
```

It can be seen that for dereference operation, we can simply decrease the
atomic variable which holds the reference. If the atomic variable holds
0 references, it can be removed no matter how many region it is shared with
(see Scenario 10).

### Scenario 6. R1 split into R1 and R3

The same to scenario 2:

```
R1 <- A1(S2, S3, S4), A2(S5)
R3 <- A1(S2, S3, S4), A2(S5)
R2 <- A1(S2, S3, S4), A3(S7)
```

### Scenario 7. R1 ref by S8

The same to scenario 2 and 3:

```
R1 <- A1(S2, S3, S4), A2(S5), A4(S8)
R3 <- A1(S2, S3, S4), A2(S5)
R2 <- A1(S2, S3, S4), A3(S7)
```

### Scenario 8. R2 ref by S9

The same to scenario 2, 3 and 7:

```
R1 <- A1(S2, S3, S4), A2(S5), A4(S8)
R3 <- A1(S2, S3, S4), A2(S5)
R2 <- A1(S2, S3, S4), A3(S7, S9)
```

### Scenario 9. R2 merge into R1

First, we naively merge all atomics:

```
R1 <- A1(S2, S3, S4), A2(S5), A4(S8), A1(S2, S3, S4), A3(S7, S9)
R3 <- A1(S2, S3, S4), A2(S5)
```

Next, same atomic instances should be de-duplicated if we want to get a
reference count by simply calculating the sum of all atomic instances:

```
R1 <- A1(S2, S3, S4), A2(S5), A4(S8), A3(S7, S9)
R3 <- A1(S2, S3, S4), A2(S5)
```

### Scenario 10. S5 drop

```
R1 <- A1(S2, S3, S4), A2(), A4(S8), A3(S7, S9)
R3 <- A1(S2, S3, S4), A2()
```

Now `A2` no longer holds reference any more. Note that because it is shared,
it cannot hold new references as well. It will be removed later when `R1`
and `R3` is changed.

*/

use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use util;
use util::collections::{HashMap, HashSet};

/// A struct represents an atomic variable, used in this module only so that it does not need
/// to be `Sync`.
struct RefHolder {
    ref_count: Arc<AtomicU64>,
}

/// `RefHolder`'s hash value is based on the pointer value in `ref_count` (see `PartialEq`).
impl Hash for RefHolder {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = Arc::into_raw(self.ref_count.clone());
        ptr.hash(state);
        let _ = unsafe { Arc::from_raw(ptr) };
    }
}

/// Two `RefHolder` are considered identical if their `ref_count` are referencing the same
/// atomic variable instance.
impl PartialEq for RefHolder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.ref_count, &other.ref_count)
    }
}

impl Eq for RefHolder {}

impl RefHolder {
    fn new() -> Self {
        Self {
            ref_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_ref_count(&self) -> u64 {
        self.ref_count.load(Ordering::SeqCst)
    }
}

/// A struct to maintain reference counter for all regions.
pub struct RegionRefCounter {
    refs: HashMap<u64, Vec<Rc<RefHolder>>>,
}

/// A struct to represent a single reference, used to send away.
/// It decrease atomic value internally when dropped.
pub struct RegionReferrer {
    ref_holder_content: Arc<AtomicU64>,
}

impl util::AssertSend for RegionReferrer {}

impl util::AssertSync for RegionReferrer {}

impl RegionReferrer {
    fn new(ref_holder: &RefHolder) -> Self {
        Self {
            ref_holder_content: ref_holder.ref_count.clone(),
        }
    }
}

impl Drop for RegionReferrer {
    fn drop(&mut self) {
        let previous_value = self.ref_holder_content.fetch_sub(1, Ordering::SeqCst);
        assert_ne!(previous_value, 0);
    }
}

impl RegionRefCounter {
    pub fn new() -> Self {
        Self {
            refs: HashMap::default(),
        }
    }

    /// Initialize reference counter for a region.
    pub fn init_region(&mut self, region_id: u64) {
        let old_value = self.refs.insert(region_id, Vec::new());
        assert!(old_value.is_none());
    }

    /// Refer a region. The region will be de-refed when `RegionReferrer` is dropped.
    pub fn ref_region(&mut self, region_id: u64) -> RegionReferrer {
        let holders = self.refs.get_mut(&region_id).unwrap();
        {
            // Find a `RefHolder` which is used only in 1 region
            let holder = holders.iter().find(|holder| Rc::strong_count(holder) == 1);
            if let Some(holder) = holder {
                return RegionReferrer::new(holder);
            }
        }

        // If there isn't any, append a new `RefHolder` and return its referrer.
        let holder = Rc::new(RefHolder::new());
        let referrer = RegionReferrer::new(&holder);
        holders.push(holder);
        referrer
    }

    /// Handle region split and update references.
    pub fn handle_region_split(&mut self, region_id: u64, new_region_id: u64) {
        let holders: Vec<_> = self.refs.remove(&region_id).unwrap()
            .into_iter()
            .filter(|holder| holder.get_ref_count() == 0) // clean up unused holders
            .collect();
        let old_value = self.refs.insert(new_region_id, holders.clone());
        assert!(old_value.is_none());
        let old_value = self.refs.insert(region_id, holders);
        assert!(old_value.is_none());
    }

    /// Handle region merge and update references.
    pub fn handle_region_merge(&mut self, src_region_id: u64, dest_region_id: u64) {
        // 1. naive merge
        let mut dest_holders = self.refs.remove(&dest_region_id).unwrap();
        let mut src_holders = self.refs.remove(&src_region_id).unwrap();
        dest_holders.append(&mut src_holders);

        // 2. deduplicate
        let dedup_dest_holders = dest_holders
            .drain(..)
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|holder| holder.get_ref_count() == 0) // clean up unused holders
            .collect::<Vec<_>>();

        // 3. insert back
        let old_value = self.refs.insert(dest_region_id, dedup_dest_holders);
        assert!(old_value.is_none());
    }

    /// Get region reference counts.
    pub fn get_region_refs(&self, region_id: u64) -> u64 {
        self.refs[&region_id]
            .iter()
            .map(|holder| holder.get_ref_count())
            .sum()
    }
}
