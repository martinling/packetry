use std::marker::PhantomData;
use std::ops::{Add, Range};

use bisection::{bisect_left, bisect_right};

use crate::data_stream::{data_stream, DataReader, DataWriter};
use crate::id::Id;
use crate::stream::StreamError;
use crate::util::{fmt_count, fmt_size};

/// Unique handle for append-only write access to an index.
pub struct IndexWriter<Position, Value> {
    marker: PhantomData<(Position, Value)>,
    data_writer: DataWriter<u64>,
}

/// Cloneable handle for read-only random access to an index.
#[derive(Clone)]
pub struct IndexReader<Position, Value> {
    marker: PhantomData<(Position, Value)>,
    data_reader: DataReader<u64>,
}

type IndexPair<P, V> = (IndexWriter<P, V>, IndexReader<P, V>);

/// Construct a new index stream.
///
/// Returns a unique writer and a cloneable reader.
///
pub fn index_stream<P, V>() -> Result<IndexPair<P, V>, StreamError> {
    let (data_writer, data_reader) = data_stream()?;
    let writer = IndexWriter {
        marker: PhantomData,
        data_writer,
    };
    let reader = IndexReader {
        marker: PhantomData,
        data_reader,
    };
    Ok((writer, reader))
}

impl<Position, Value> IndexWriter<Position, Value>
where Position: From<u64>, Value: Into<u64>
{
    /// Number of entries in the index.
    pub fn len(&self) -> u64 {
        self.data_writer.len()
    }

    /// Size of the index in bytes.
    pub fn size(&self) -> u64 {
        self.data_writer.size()
    }

    /// Add a single value to the end of the index.
    ///
    /// Returns the position of the added value.
    pub fn push(&mut self, value: Value) -> Result<Position, StreamError> {
        let id = self.data_writer.push(&value.into())?;
        let position = Position::from(id.into());
        Ok(position)
    }
}

impl<Position, Value> IndexReader<Position, Value>
where Position: Copy + From<u64> + Into<u64> + Add<u64, Output=Position>,
      Value: Copy + From<u64> + Ord
{
    /// Current number of indices in the index.
    pub fn len(&self) -> u64 {
        self.data_reader.len()
    }

    /// Current size of the index in bytes.
    pub fn size(&self) -> u64 {
        self.data_reader.size()
    }

    /// Get a single value from the index, by position.
    pub fn get(&mut self, position: Position) -> Result<Value, StreamError> {
        let id = Id::<u64>::from(position.into());
        let value = self.data_reader.get(id)?;
        Ok(Value::from(value))
    }

    /// Get multiple values from the index, for a range of positions.
    pub fn get_range(&mut self, range: &Range<Position>)
        -> Result<Vec<Value>, StreamError>
    {
        let start = Id::<u64>::from(range.start.into());
        let end = Id::<u64>::from(range.end.into());
        let data = self.data_reader.get_range(&(start..end))?;
        let values = unsafe {
            std::mem::transmute::<Vec<u64>, Vec<Value>>(data)
        };
        Ok(values)
    }

    /// Leftmost position where a value would be ordered within this index.
    pub fn bisect_left(&mut self, value: &Value)
        -> Result<Position, StreamError>
    {
        let range = Position::from(0)..Position::from(self.len());
        let values = self.get_range(&range)?;
        let position = Position::from(bisect_left(&values, value) as u64);
        Ok(position)
    }

    /// Rightmost position where a value would be ordered within this index.
    pub fn bisect_right(&mut self, value: &Value)
        -> Result<Position, StreamError>
    {
        let range = Position::from(0)..Position::from(self.len());
        let values = self.get_range(&range)?;
        let position = Position::from(bisect_right(&values, value) as u64);
        Ok(position)
    }
}

impl<Position, Value> std::fmt::Display for IndexWriter<Position, Value>
where Position: From<u64>, Value: Into<u64>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} entries, {}", fmt_count(self.len()), fmt_size(self.size()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_stream() {
        let (mut writer, mut reader) = index_stream().unwrap();
        let mut expected = Vec::<Id<u8>>::new();
        let mut x = 10;
        let n = 321;
        for i in 0..n {
            x += 1 + i % 3;
            let id = Id::<u8>::from(x);
            expected.push(id);
            writer.push(id).unwrap();
        }
        for i in 0..n {
            let id = Id::<Id<u8>>::from(i);
            let vi = reader.get(id).unwrap();
            let xi = expected[i as usize];
            assert!(vi == xi);
        }
        let end = Id::<Id<u8>>::from(n as u64);
        for i in 0..n {
            let start = Id::<Id<u8>>::from(i as u64);
            let vrng = start .. end;
            let xrng = i as usize .. n as usize;
            let vr = reader.get_range(&vrng).unwrap();
            let xr = &expected[xrng];
            assert!(vr == xr);
        }
        let start = Id::<Id<u8>>::from(0 as u64);
        for i in 0..n {
            let end = Id::<Id<u8>>::from(i as u64);
            let vrng = start .. end;
            let xrng = 0 as usize .. i as usize;
            let vr = reader.get_range(&vrng).unwrap();
            let xr = &expected[xrng];
            assert!(vr == xr);
        }
        for i in 0..(n - 10) {
            let start = Id::<Id<u8>>::from(i as u64);
            let end = Id::<Id<u8>>::from(i + 10 as u64);
            let vrng = start .. end;
            let xrng = i as usize .. (i + 10) as usize;
            let vr = reader.get_range(&vrng).unwrap();
            let xr = &expected[xrng];
            assert!(vr == xr);
        }
    }
}
