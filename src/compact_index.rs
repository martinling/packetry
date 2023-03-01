use std::cmp::max;
use std::iter::once;
use std::marker::PhantomData;
use std::ops::{Add, Range, Sub};
use std::sync::atomic::{AtomicU64, Ordering::{Acquire, Release}};
use std::sync::Arc;

use bisection::bisect_left;
use itertools::multizip;

use crate::data_stream::{data_stream, DataReader, DataWriter};
use crate::id::Id;
use crate::index_stream::{index_stream, IndexReader, IndexWriter};
use crate::stream::StreamError;
use crate::util::{fmt_count, fmt_size};

type Offset = Id<u8>;
type SegmentId = u64;

/// Unique handle for append-only write access to a compact index stream.
pub struct CompactWriter<Position, Value, const MIN_WIDTH: usize = 1> {
    /// Committed length of this index available to readers.
    shared_length: Arc<AtomicU64>,
    /// Index of starting positions of each segment.
    segment_start_writer: IndexWriter<SegmentId, Position>,
    /// Index of data offsets of each segment.
    segment_offset_writer: IndexWriter<SegmentId, Offset>,
    /// Stream of compressed segments (base value, delta width, deltas).
    data_writer: DataWriter<u8>,
    /// Current write position in the data stream.
    data_offset: Offset,
    /// Base value of the current segment, if there is one.
    current_base_value: Option<Value>,
    /// Delta width of the current segment, if there is one.
    current_delta_width: Option<usize>,
    /// Master length of the index.
    length: u64,
}

/// Cloneable handle for read-only random access to a compact index stream.
#[derive(Clone)]
pub struct CompactReader<Position, Value> {
    _marker: PhantomData<Value>,
    /// Committed length of this index available to readers.
    shared_length: Arc<AtomicU64>,
    /// Index of starting positions of each segment.
    segment_start_reader: IndexReader<SegmentId, Position>,
    /// Index of data offsets of each segment.
    segment_offset_reader: IndexReader<SegmentId, Offset>,
    /// Stream of compressed segments (base value, delta width, deltas).
    data_reader: DataReader<u8>,
}

type CompactPair<P, V, const W: usize> =
    (CompactWriter<P, V, W>, CompactReader<P, V>);

/// Construct a new index stream.
///
/// Returns a unique writer and a cloneable reader.
///
pub fn compact_index<P, V, const W: usize>()
    -> Result<CompactPair<P, V, W>, StreamError>
{
    let (segment_start_writer, segment_start_reader) = index_stream()?;
    let (segment_offset_writer, segment_offset_reader) = index_stream()?;
    let (data_writer, data_reader) = data_stream()?;
    let shared_length = Arc::new(AtomicU64::from(0));
    let writer = CompactWriter {
        shared_length: shared_length.clone(),
        segment_start_writer,
        segment_offset_writer,
        data_writer,
        data_offset: Offset::from(0),
        current_base_value: None,
        current_delta_width: None,
        length: 0,
    };
    let reader = CompactReader {
        _marker: PhantomData,
        shared_length,
        segment_start_reader,
        segment_offset_reader,
        data_reader,
    };
    Ok((writer, reader))
}

impl<Position, Value, const MIN_WIDTH: usize>
CompactWriter<Position, Value, MIN_WIDTH>
where Position: Copy + From<u64> + Into<u64>,
      Value: Copy + Into<u64> + Sub<Output=u64>
{
    /// Current number of entries in the index.
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Current size of the index in bytes.
    pub fn size(&self) -> u64 {
        self.segment_start_writer.size() +
            self.segment_offset_writer.size() +
            self.data_writer.size()
    }

    /// Add a single value to the end of the index.
    ///
    /// Returns the position of the added value.
    pub fn push(&mut self, value: Value) -> Result<Position, StreamError> {
        match self.current_base_value {
            None => self.start_segment(value)?,
            Some(current_base_value) => {
                let delta = value - current_base_value;
                let delta_width = max(byte_width(delta), MIN_WIDTH);
                match self.current_delta_width {
                    None => {
                        let delta_bytes = delta.to_le_bytes();
                        self.data_writer.push(&(delta_width as u8))?;
                        self.data_writer.append(&delta_bytes[..delta_width])?;
                        self.data_offset += 1 + delta_width as u64;
                        self.current_delta_width = Some(delta_width);
                    },
                    Some(current_width) if delta_width > current_width => {
                        self.start_segment(value)?;
                    },
                    Some(current_width) => {
                        let delta_bytes = delta.to_le_bytes();
                        self.data_writer.append(&delta_bytes[..current_width])?;
                        self.data_offset += current_width as u64;
                    }
                }
            }
        }
        let position = Position::from(self.length);
        self.length += 1;
        self.shared_length.store(self.length, Release);
        Ok(position)
    }

    fn start_segment(&mut self, base_value: Value) -> Result<(), StreamError> {
        let segment_start = Position::from(self.length);
        let base_value_bytes = base_value.into().to_le_bytes();
        self.segment_start_writer.push(segment_start)?;
        self.segment_offset_writer.push(self.data_offset)?;
        self.data_writer.append(&base_value_bytes)?;
        self.data_offset += base_value_bytes.len() as u64;
        self.current_base_value = Some(base_value);
        self.current_delta_width = None;
        Ok(())
    }
}

impl<Position, Value> CompactReader<Position, Value>
where
    Position: Copy + From<u64> + Into<u64> + Ord + Sub<Output=u64>
        + Add<u64, Output=Position> + Sub<u64, Output=Position>,
    Value: Copy + From<u64> + Into<u64> + Ord + Sub<Output=u64>
{
    /// Number of entries in the index.
    pub fn len(&self) -> u64 {
        self.shared_length.load(Acquire)
    }

    /// Size of the index in bytes.
    pub fn size(&self) -> u64 {
        self.segment_start_reader.size() +
            self.segment_offset_reader.size() +
            self.data_reader.size()
    }

    /// Get a single value from the index, by position.
    pub fn get(&mut self, position: Position) -> Result<Value, StreamError> {
        // Find the segment required.
        let segment_id = self.segment_start_reader.bisect_right(&position)? - 1;
        let segment_start = self.segment_start_reader.get(segment_id)?;
        let segment_offset = self.segment_offset_reader.get(segment_id)?;
        // If fetching the base value only, we don't need the delta width.
        let base_only = position == segment_start;
        let header_length = if base_only { 8 } else { 9 };
        // Fetch the segment header and get the base value.
        let header_range = segment_offset..(segment_offset + header_length);
        let header_bytes = self.data_reader.get_range(&header_range)?;
        let base_bytes: [u8; 8] = header_bytes[0..8].try_into().unwrap();
        let base_value = u64::from_le_bytes(base_bytes);
        // If we only need the base value, return it.
        if base_only {
            return Ok(Value::from(base_value))
        }
        // Otherwise, identify the delta we need and fetch it.
        let width = header_bytes[8] as usize;
        let delta_index = position - segment_start - 1;
        let delta_start = header_range.end + delta_index * width as u64;
        let delta_range = delta_start..(delta_start + width as u64);
        let delta_low_bytes = self.data_reader.get_range(&delta_range)?;
        // Reconstruct the delta and the complete value.
        let mut delta_bytes = [0; 8];
        delta_bytes[..width].copy_from_slice(delta_low_bytes.as_slice());
        let delta = u64::from_le_bytes(delta_bytes);
        Ok(Value::from(base_value + delta))
    }

    /// Get multiple values from the index, for a range of positions.
    pub fn get_range(&mut self, range: &Range<Position>)
        -> Result<Vec<Value>, StreamError>
    {
        // Allocate space for the result.
        let total_count: usize = (range.end - range.start).try_into().unwrap();
        let mut data = Vec::with_capacity(total_count);
        // Determine which segments we need to read from.
        let first = self.segment_start_reader.bisect_right(&range.start)? - 1;
        let last = self.segment_start_reader.bisect_left(&range.end)? - 1;
        let seg_range = first..(last + 1);
        let segment_starts = self.segment_start_reader.get_range(&seg_range)?;
        let segment_offsets = self.segment_offset_reader.get_range(&seg_range)?;
        // Iterate over the segments.
        for (segment_start, segment_offset, start_position, end_position) in
            multizip((
                // The starting position of each segment.
                segment_starts.iter(),
                // The data offset of each segment.
                segment_offsets.into_iter(),
                // The start of the positions we need to read from each segment.
                once(&range.start).chain(segment_starts.iter().skip(1)),
                // The end of the positions we need to read from each segment.
                segment_starts.iter().chain(once(&range.end)).skip(1)))
        {
            // Count how many values we need to retrieve from this segment.
            let this_segment_count = *end_position - *start_position;
            // Check if we are including the base value of this segment.
            let base_included = *start_position == *segment_start;
            // If fetching the base value only, we don't need the delta width.
            let base_only = base_included && this_segment_count == 1;
            let header_length = if base_only { 8 } else { 9 };
            // Fetch the segment header and get the base value.
            let header_range = segment_offset..(segment_offset + header_length);
            let header_bytes = self.data_reader.get_range(&header_range)?;
            let base_bytes: [u8; 8] = header_bytes[0..8].try_into().unwrap();
            let base_value = u64::from_le_bytes(base_bytes);
            // Include the base value in the result if needed.
            if base_included {
                data.push(base_value);
                // If we only needed the base value, proceed to next segment.
                if base_only {
                    continue;
                }
            }
            // Otherwise, get width from header.
            let width = header_bytes[8] as usize;
            // Get delta range required.
            let (delta_start, num_deltas) = if base_included {
                // Deltas start right after header, and we need one fewer.
                (header_range.end, this_segment_count - 1)
            } else {
                // Deltas start at an offset, and we need one for every value.
                let first_delta = *start_position - *segment_start - 1;
                let offset = header_range.end + first_delta * width as u64;
                (offset, this_segment_count)
            };
            let delta_end = delta_start + num_deltas * width as u64;
            let delta_range = delta_start..delta_end;
            // Fetch all the required delta bytes for this segment.
            let all_delta_bytes = self.data_reader.get_range(&delta_range)?;
            // Reconstruct deltas and values to include in result.
            let mut delta_bytes = [0; 8];
            for low_bytes in all_delta_bytes.chunks_exact(width) {
                delta_bytes[..width].copy_from_slice(low_bytes);
                let delta = u64::from_le_bytes(delta_bytes);
                assert!(data.len() < total_count);
                data.push(base_value + delta);
            }
        }
        assert!(data.len() == total_count);
        let values = unsafe {
            std::mem::transmute::<Vec<u64>, Vec<Value>>(data)
        };
        Ok(values)
    }

    /// Get the range of values between the specified position and the next.
    ///
    /// The length of the data referenced by this index must be passed
    /// as a parameter. If the specified position is the last in the
    /// index, the range will be from the last value in the index to the
    /// end of the referenced data.
    pub fn target_range(&mut self, position: Position, target_length: u64)
        -> Result<Range<Value>, StreamError>
    {
        let range = if position.into() + 2 > self.len() {
            let start = self.get(position)?;
            let end = Value::from(target_length);
            start..end
        } else {
            let vec = self.get_range(&(position..(position + 2)))?;
            let start = vec[0];
            let end = vec[1];
            start..end
        };
        Ok(range)
    }

    /// Leftmost position where a value would be ordered within this range.
    pub fn bisect_range_left(&mut self, range: &Range<Position>, value: &Value)
        -> Result<Position, StreamError>
    {
        let values = self.get_range(range)?;
        let position = Position::from(bisect_left(&values, value) as u64);
        Ok(position)
    }
}

fn byte_width(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        (8 - value.leading_zeros() / 8) as usize
    }
}

impl<Position, Value, const MIN_WIDTH: usize>
std::fmt::Display for CompactWriter<Position, Value, MIN_WIDTH>
where Position: Copy + From<u64> + Into<u64>,
      Value: Copy + Into<u64> + Sub<Output=u64>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} entries in {} segments, {}",
               fmt_count(self.len()),
               fmt_count(self.segment_start_writer.len()),
               fmt_size(self.size()))
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
    fn test_compact_index() {
        let (mut writer, mut reader) = index_stream().unwrap();
        let mut expected = Vec::<Id<u8>>::new();
        let mut x = 10;
        let n = 4321;
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
