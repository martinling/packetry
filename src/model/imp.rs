//! Defines the implementation of our model

use gio::subclass::prelude::*;
use gtk::{gio, glib, prelude::*};

use crate::capture::{self, Capture, CaptureError};

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use crate::row_data::DeviceRowData;

use thiserror::Error;

#[derive(Default)]
pub struct DeviceModel {
    pub(super) capture: RefCell<Option<Arc<Mutex<Capture>>>>,
    pub(super) parent: RefCell<Option<capture::DeviceItem>>,
}

#[glib::object_subclass]
impl ObjectSubclass for DeviceModel {
    const NAME: &'static str = "DeviceModel";
    type Type = super::DeviceModel;
    type Interfaces = (gio::ListModel,);

}

#[derive(Error, Debug)]
pub enum ModelError {
    #[error(transparent)]
    CaptureError(#[from] CaptureError),
    #[error("Capture not set")]
    CaptureNotSet,
    #[error("Locking capture failed")]
    LockError,
}

impl DeviceModel {
    fn try_n_items(&self)
        -> Result<u32, ModelError>
    {
        let opt = self.capture.borrow();
        let mut cap = match opt.as_ref() {
            Some(mutex) => match mutex.lock() {
                Ok(guard) => guard,
                Err(_) => return Err(ModelError::LockError)
            },
            None => return Err(ModelError::CaptureNotSet)
        };
        Ok(cap.device_item_count(&self.parent.borrow())? as u32)
    }

    fn try_item(&self, position: u32)
        -> Result<Option<glib::Object>, ModelError>
    {
        let opt = self.capture.borrow();
        let mut cap = match opt.as_ref() {
            Some(mutex) => match mutex.lock() {
                Ok(guard) => guard,
                Err(_) => return Err(ModelError::LockError)
            },
            None => return Err(ModelError::CaptureNotSet)
        };
        let item = cap.get_device_item(&self.parent.borrow(),
                                       position as u64)?;
        let summary = match cap.get_device_summary(&item) {
            Ok(string) => string,
            Err(e) => format!("Error: {:?}", e)
        };
        Ok(Some(DeviceRowData::new(Some(item), summary)
                              .upcast::<glib::Object>()))
    }
}

impl ObjectImpl for DeviceModel {}

impl ListModelImpl for DeviceModel {
    fn item_type(&self, _list_model: &Self::Type) -> glib::Type {
        DeviceRowData::static_type()
    }

    fn n_items(&self, _list_model: &Self::Type) -> u32 {
        self.try_n_items().unwrap_or(0)
    }

    fn item(&self, _list_model: &Self::Type, position: u32)
        -> Option<glib::Object>
    {
        self.try_item(position).unwrap_or(None)
    }
}
