use std::fs::File;
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, AtomicU32, Ordering},
    Arc,
};

use anyhow::Error;
use arc_swap::{ArcSwap, ArcSwapOption};

use crate::util::id::Id;

pub trait Dump : Sized {
    /// Dump the contents of this data structure to the specified path.
    fn dump(&self, dest: &Path) -> Result<(), Error>;

    /// Restore a data structure of this type from the specified path.
    fn restore(src: &Path) -> Result<Self, Error>;
}

/// Standalone function to restore any type that supports Dump.
pub fn restore<T>(src: &Path) -> Result<T, Error> where T: Dump {
    T::restore(src)
}

impl Dump for String {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        let mut file = File::create(dest)?;
        writeln!(file, "{}", self)?;
        Ok(())
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        let mut string = String::new();
        File::open(src)?.read_to_string(&mut string)?;
        Ok(string.trim_end_matches("\n").to_string())
    }
}

impl Dump for usize {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.to_string().dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(Self::from_str(&String::restore(src)?)?)
    }
}

impl Dump for u64 {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.to_string().dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(Self::from_str(&String::restore(src)?)?)
    }
}

impl Dump for u32 {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.to_string().dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(Self::from_str(&String::restore(src)?)?)
    }
}

impl Dump for AtomicU64 {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.load(Ordering::Acquire).dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(AtomicU64::from(u64::restore(src)?))
    }
}

impl Dump for AtomicU32 {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.load(Ordering::Acquire).dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(AtomicU32::from(u32::restore(src)?))
    }
}

impl<T> Dump for Id<T> {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.value.dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(Self::from(u64::restore(src)?))
    }
}

impl<T> Dump for Option<T> where T: Dump {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        if let Some(value) = self {
            value.dump(dest)?
        }
        Ok(())
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        match T::restore(src) {
            Ok(value) => Ok(Some(value)),
            Err(e) => match e.root_cause().downcast_ref::<std::io::Error>() {
                Some(io_error) => match io_error.kind() {
                    std::io::ErrorKind::NotFound => Ok(None),
                    _ => Err(e)
                },
                _ => Err(e)
            }
        }
    }
}

impl<T> Dump for Arc<T> where T: Dump {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.deref().dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(Arc::new(T::restore(src)?))
    }
}

impl<T> Dump for ArcSwapOption<T> where T: Dump {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.load_full().dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(ArcSwapOption::new(
            Option::<T>::restore(src)?.map(|value| Arc::new(value))
        ))
    }
}

impl<T> Dump for ArcSwap<T> where T: Dump {
    fn dump(&self, dest: &Path) -> Result<(), Error> {
        self.load_full().dump(dest)
    }

    fn restore(src: &Path) -> Result<Self, Error> {
        Ok(ArcSwap::new(Arc::new(T::restore(src)?)))
    }
}
