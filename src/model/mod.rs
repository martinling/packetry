//! Defines our custom model

mod imp;

use std::sync::{Arc, Mutex};

use gtk::subclass::prelude::*;
use gtk::prelude::ListModelExt;
use gtk::{gio, glib};

use crate::capture::{Capture, TrafficItem, DeviceItem};
use crate::tree_list_model::{TreeListModel, ItemRc, ModelError};

// Public part of the Model type.
glib::wrapper! {
    pub struct TrafficModel(ObjectSubclass<imp::TrafficModel>) @implements gio::ListModel;
}
glib::wrapper! {
    pub struct DeviceModel(ObjectSubclass<imp::DeviceModel>) @implements gio::ListModel;
}

pub trait GenericModel<Item> where Self: Sized {
    fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError>;
    fn set_expanded(&self, node: &ItemRc<Item>, position: u32, expanded: bool)
        -> Result<(), ModelError>;
}

impl GenericModel<TrafficItem> for TrafficModel {
    fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        let model: TrafficModel =
            glib::Object::new(&[]).expect("Failed to create TrafficModel");
        let tree = TreeListModel::new(capture)?;
        model.imp().tree.replace(Some(tree));
        Ok(model)
    }

    fn set_expanded(&self,
                    node: &ItemRc<TrafficItem>,
                    position: u32,
                    expanded: bool)
        -> Result<(), ModelError>
    {
        let update = self.imp().tree
            .borrow_mut()
            .as_mut()
            .unwrap()
            .set_expanded(node, position as u64, expanded)?;

        self.items_changed(
            update.position, update.rows_removed, update.rows_added);

        Ok(())
    }
}

impl GenericModel<DeviceItem> for DeviceModel {
    fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        let model: DeviceModel =
            glib::Object::new(&[]).expect("Failed to create DeviceModel");
        let tree = TreeListModel::new(capture)?;
        model.imp().tree.replace(Some(tree));
        Ok(model)
    }

    fn set_expanded(&self,
                    node: &ItemRc<DeviceItem>,
                    position: u32,
                    expanded: bool)
        -> Result<(), ModelError>
    {
        let update = self.imp().tree
            .borrow_mut()
            .as_mut()
            .unwrap()
            .set_expanded(node, position as u64, expanded)?;

        self.items_changed(
            update.position, update.rows_removed, update.rows_added);

        Ok(())
    }
}
