use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use std::marker::PhantomData;
use std::ops::{Deref, Range};

use bytemuck::{bytes_of, pod_read_unaligned, Pod};
use memmap2::{Mmap, MmapOptions};
use tempfile::tempfile;
use thiserror::Error;

use crate::id::Id;

#[derive(Error, Debug)]
pub enum FileVecError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

pub struct FileVec<T> where T: Pod + Default {
    _marker: PhantomData<T>,
    writer: BufWriter<File>,
    file_length: u64,
    item_count: u64,
    map_length: usize,
    map: Option<Mmap>,
}

impl<T: Pod + Default> FileVec<T> {
   pub fn new() -> Result<Self, FileVecError> {
        let file = tempfile()?;
        Ok(Self{
            _marker: PhantomData,
            writer: BufWriter::new(file),
            file_length: 0,
            item_count: 0,
            map_length: 0,
            map: None,
        })
    }

    pub fn push(&mut self, item: &T)
       -> Result<Id<T>, FileVecError> where T: Pod
    {
        let data= bytes_of(item);
        self.writer.write_all(data)?;
        self.file_length += data.len() as u64;
        let id = Id::<T>::from(self.item_count);
        self.item_count += 1;
        Ok(id)
    }

    pub fn append(&mut self, items: &[T])
       -> Result<Id<T>, FileVecError> where T: Pod
    {
        for item in items {
            let data = bytes_of(item);
            self.writer.write_all(data)?;
            self.file_length += data.len() as u64;
        }
        let id = Id::<T>::from(self.item_count);
        self.item_count += items.len() as u64;
        Ok(id)
    }

    fn access(&mut self, range: &Range<Id<T>>)
        -> Result<&impl Deref<Target=[u8]>, FileVecError>
    {
        let size = std::mem::size_of::<T>() as usize;
        let end = size * range.end.value as usize;
        if end > self.map_length {
            self.writer.flush()?;
            let file = self.writer.get_ref();
            unsafe {
               self.map = Some(MmapOptions::new().map(file)?);
            }
            self.map_length = self.file_length as usize;
        }
        Ok(self.map.as_ref().unwrap())
    }

    pub fn get(&mut self, id: Id<T>) -> Result<T, FileVecError> {
        let range = id..(id + 1);
        let data = self.access(&range)?;
        let size = std::mem::size_of::<T>() as usize;
        let start = size * id.value as usize;
        let end = start + size;
        Ok(pod_read_unaligned::<T>(&data[start..end]))
    }

    pub fn get_range(&mut self, range: Range<Id<T>>) -> Result<Vec<T>, FileVecError> {
        let mut result = Vec::new();
        let data = self.access(&range)?;
        let size = std::mem::size_of::<T>() as usize;
        for i in range.start.value..range.end.value {
            let start = size * i as usize;
            let end = start + size;
            result.push(pod_read_unaligned::<T>(&data[start..end]))
        }
        Ok(result)
    }

    pub fn len(&self) -> u64 {
        self.item_count
    }

    pub fn size(&self) -> u64 {
       self.file_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck_derive::{Pod, Zeroable};

    #[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Pod, Zeroable)]
    #[repr(C)]
    struct Foo {
        bar: u32,
        baz: u32,
    }

    #[test]
    fn test_file_vec_push() {
        let mut v = FileVec::new().unwrap();
        for i in 0..100 {
            let x= Foo{ bar: i, baz: i};
            v.push(&x).unwrap();
            assert!(v.get(Id::<Foo>::from(i as u64)).unwrap() == x);
        }
    }

    #[test]
    fn test_file_vec_append() {
        let mut file_vec = FileVec::new().unwrap();

        // Build a (normal) Vec of data
        let mut data = Vec::new();
        for i in 0..100 {
            let item= Foo{ bar: i, baz: i};
            data.push(item)
        }

        // append it to the FileVec
        file_vec.append(&data.as_slice()).unwrap();

        // and check
        let start = Id::<Foo>::from(0);
        let end = Id::<Foo>::from(100);
        let range = start .. end;
        let vec: Vec<_> = file_vec.get_range(range).unwrap();
        assert!(vec == data);
    }
}
