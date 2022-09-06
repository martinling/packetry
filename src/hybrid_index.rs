use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use std::ops::{Range, Deref};
use std::cmp::{min, max};
use std::marker::PhantomData;

use tempfile::tempfile;
use thiserror::Error;
use bisection::{bisect_left, bisect_right};
use memmap2::{Mmap, MmapOptions};

use crate::id::Id;

#[derive(Error, Debug)]
pub enum HybridIndexError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

bitfield! {
    pub struct IncrementFields(u64);
    u64, count, set_count: 59, 0;
    u8, width, set_width: 63, 60;
}

impl IncrementFields {
    fn size(&self) -> u64 {
        self.count() * self.width() as u64
    }
}

struct Entry {
    base_value: u64,
    file_offset: u64,
    increments: IncrementFields,
}

pub trait Number {
    fn from_u64(i: u64) -> Self;
    fn to_u64(&self) -> u64;
}

impl Number for u64 {
    fn from_u64(i: u64) -> Self { i }
    fn to_u64(&self) -> u64 { *self }
}

impl<T> Number for Id<T> {
    fn from_u64(i: u64) -> Self { Id::<T>::from(i) }
    fn to_u64(&self) -> u64 { self.value }
}

pub struct HybridIndex<I, T> {
    _marker: PhantomData<(I, T)>,
    min_width: u8,
    writer: BufWriter<File>,
    file_length: u64,
    total_count: u64,
    entries: Vec<Entry>,
    index: Vec<u64>,
    last_value: u64,
    mappings: BTreeMap<u64, Mmap>,
}

impl<I: Number, T: Number + Copy + Ord> HybridIndex<I, T> {
    pub fn new(min_width: u8) -> Result<Self, HybridIndexError> {
        let file = tempfile()?;
        Ok(Self{
            _marker: PhantomData,
            min_width,
            writer: BufWriter::new(file),
            file_length: 0,
            total_count: 0,
            entries: Vec::new(),
            index: Vec::new(),
            last_value: 0,
            mappings: BTreeMap::new(),
        })
    }

    pub fn push(&mut self, id: T) -> Result<I, HybridIndexError>
    {
        if self.entries.is_empty() {
            let first_entry = Entry {
                base_value: id.to_u64(),
                file_offset: 0,
                increments: IncrementFields(0),
            };
            self.entries.push(first_entry);
            self.index.push(0);
        } else {
            let last_entry = self.entries.last_mut().unwrap();
            let increment = id.to_u64() - last_entry.base_value;
            let width = max(byte_width(increment), self.min_width);
            let count = last_entry.increments.count();
            if count > 0 && width > last_entry.increments.width() {
                let new_entry = Entry {
                    base_value: id.to_u64(),
                    file_offset: self.file_length,
                    increments: IncrementFields(0),
                };
                self.entries.push(new_entry);
                self.index.push(self.total_count);
            } else {
                if last_entry.increments.width() == 0 {
                    last_entry.increments.set_width(width);
                }
                let bytes = increment.to_le_bytes();
                self.writer.write_all(&bytes[0..width as usize])?;
                self.file_length += width as u64;
                last_entry.increments.set_count(count + 1);
            }
        }
        let new_id = I::from_u64(self.total_count);
        self.total_count += 1;
        self.last_value = id.to_u64();
        Ok(new_id)
    }

    fn access(&mut self, entry_id: usize)
        -> Result<(&Entry, &impl Deref<Target=[u8]>), HybridIndexError>
    {
        use std::collections::btree_map::Entry::Vacant;
        let entry = &self.entries[entry_id];
        if let Vacant(vacant) = self.mappings.entry(entry.file_offset) {
            self.writer.flush()?;
            let file = self.writer.get_ref();
            vacant.insert(
                unsafe {
                    MmapOptions::new()
                        .offset(entry.file_offset)
                        .len(entry.increments.size() as usize)
                        .map(file)?
                }
            );
        }
        let block = self.mappings.get(&entry.file_offset).unwrap();
        Ok((entry, block))
    }

    pub fn get(&mut self, id: I) -> Result<T, HybridIndexError> {
        let id_value = id.to_u64();
        let entry_id = bisect_right(self.index.as_slice(), &id_value) - 1;
        let entry = &self.entries[entry_id];
        let increment_id = id_value - self.index[entry_id];
        if increment_id == 0 {
            Ok(<T>::from_u64(entry.base_value))
        } else {
            let (entry, block) = self.access(entry_id)?;
            let width = entry.increments.width() as usize;
            let start = width * (increment_id - 1) as usize;
            let end = start + width as usize;
            let mut bytes = [0_u8; 8];
            bytes[0..width].copy_from_slice(&block[start..end]);
            let increment = u64::from_le_bytes(bytes);
            let value = entry.base_value + increment;
            Ok(<T>::from_u64(value))
        }
    }

    pub fn get_range(&mut self, range: &Range<I>)
        -> Result<Vec<T>, HybridIndexError>
    {
        let mut result = Vec::new();
        let mut i = range.start.to_u64();
        let end = range.end.to_u64();
        while i < end {
            let entry_id = bisect_right(self.index.as_slice(), &i) - 1;
            let entry = &self.entries[entry_id];
            let mut increment_id = i - self.index[entry_id];
            if increment_id == 0 {
                result.push(<T>::from_u64(entry.base_value));
                i += 1;
            } else {
                increment_id -= 1;
            }
            let available = entry.increments.count() - increment_id;
            let needed = end - i;
            let read_count = min(available, needed);
            if read_count == 0 {
                continue;
            }
            let width = entry.increments.width() as usize;
            let (entry, block) = self.access(entry_id)?;
            let mut start = increment_id as usize * width;
            for _ in 0..read_count {
                let mut bytes = [0_u8; 8];
                let end = start + width;
                bytes[0..width].copy_from_slice(&block[start..end]);
                let increment = u64::from_le_bytes(bytes);
                let value = entry.base_value + increment;
                result.push(<T>::from_u64(value));
                start += width;
            }
            i += read_count;
        }
        Ok(result)
    }

    pub fn target_range(&mut self, id: I, target_length: u64)
        -> Result<Range<T>, HybridIndexError>
    {
        let id_value = id.to_u64();
        Ok(if id_value + 2 > self.len() {
            let start = self.get(id)?;
            let end = <T>::from_u64(target_length);
            start..end
        } else {
            let limit = I::from_u64(id_value + 2);
            let vec = self.get_range(&(id .. limit))?;
            let start = vec[0];
            let end = vec[1];
            start..end
        })
    }

    pub fn bisect(&mut self, range: &Range<I>, value: &T)
        -> Result<u64, HybridIndexError>
    {
        let values = self.get_range(range)?;
        Ok(bisect_left(&values, value).try_into().unwrap())
    }

    pub fn len(&self) -> u64 {
        self.total_count
    }

    pub fn entry_count(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn size(&self) -> u64 {
       self.file_length +
           self.entries.len() as u64 * std::mem::size_of::<Entry>() as u64 +
           self.index.len() as u64 * std::mem::size_of::<u64>() as u64
    }
}

fn byte_width(value: u64) -> u8 {
    if value == 0 {
        1
    } else {
        (8 - value.leading_zeros() / 8) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_width() {
        assert!(byte_width(0x000000) == 1);
        assert!(byte_width(0x000001) == 1);
        assert!(byte_width(0x0000FF) == 1);
        assert!(byte_width(0x000100) == 2);
        assert!(byte_width(0x000101) == 2);
        assert!(byte_width(0x00FFFF) == 2);
        assert!(byte_width(0x010000) == 3);
        assert!(byte_width(0x010001) == 3);
        assert!(byte_width(0xFFFFFF) == 3);
    }

    #[test]
    fn test_hybrid_index() {
        let mut v = HybridIndex::new(1).unwrap();
        let mut expected = Vec::<Id<u8>>::new();
        let mut x = 10;
        let n = 321;
        for i in 0..n {
            x += 1 + i % 3;
            let id = Id::<u8>::from(x);
            expected.push(id);
            v.push(id).unwrap();
        }
        for i in 0..n {
            let id = Id::<Id<u8>>::from(i);
            let vi = v.get(id).unwrap();
            let xi = expected[i as usize];
            assert!(vi == xi);
        }
        let end = Id::<Id<u8>>::from(n as u64);
        for i in 0..n {
            let start = Id::<Id<u8>>::from(i as u64);
            let vrng = start .. end;
            let xrng = i as usize .. n as usize;
            let vr = v.get_range(&vrng).unwrap();
            let xr = &expected[xrng];
            assert!(vr == xr);
        }
        let start = Id::<Id<u8>>::from(0 as u64);
        for i in 0..n {
            let end = Id::<Id<u8>>::from(i as u64);
            let vrng = start .. end;
            let xrng = 0 as usize .. i as usize;
            let vr = v.get_range(&vrng).unwrap();
            let xr = &expected[xrng];
            assert!(vr == xr);
        }
        for i in 0..(n - 10) {
            let start = Id::<Id<u8>>::from(i as u64);
            let end = Id::<Id<u8>>::from(i + 10 as u64);
            let vrng = start .. end;
            let xrng = i as usize .. (i + 10) as usize;
            let vr = v.get_range(&vrng).unwrap();
            let xr = &expected[xrng];
            assert!(vr == xr);
        }
    }
}
