//! Defines our custom model

mod imp;

use std::sync::{Arc, Mutex};
use std::cmp::min;

use gtk::subclass::prelude::*;
use gtk::prelude::ListModelExt;
use gtk::{gio, glib};

use crate::capture::{Capture, TrafficItem, DeviceItem};
use crate::tree_list_model::{TreeListModel, ItemNode, ItemRc, ModelError};

pub const MAX_ROWS: u64 = u32::MAX as u64;

pub fn clamp(value: u64, maximum: u64) -> u32 {
    min(value, maximum) as u32
}

// Public part of the Model type.
glib::wrapper! {
    pub struct TrafficModel(ObjectSubclass<imp::TrafficModel>) @implements gio::ListModel;
}
glib::wrapper! {
    pub struct DeviceModel(ObjectSubclass<imp::DeviceModel>) @implements gio::ListModel;
}

pub trait GenericModel<Item> where Self: Sized {
    fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError>;
    fn expandable(&self, node: &ItemNode<Item>) -> Result<bool, ModelError>;
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

    fn expandable(&self, node: &ItemNode<TrafficItem>)
        -> Result<bool, ModelError>
    {
        self.imp().tree
            .borrow_mut()
            .as_mut()
            .unwrap()
            .expandable(node)
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

        let position = position + 1;
        let rows_addressable = MAX_ROWS - position as u64;
        let rows_removed = clamp(
            update.rows_removed + update.rows_changed,
            rows_addressable);
        let rows_added = clamp(
            update.rows_added + update.rows_changed,
            rows_addressable);

        self.items_changed(
            position as u32,
            rows_removed as u32,
            rows_added as u32);

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

    fn expandable(&self, node: &ItemNode<DeviceItem>)
        -> Result<bool, ModelError>
    {
        self.imp().tree
            .borrow_mut()
            .as_mut()
            .unwrap()
            .expandable(node)
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

        let position = position + 1;
        let rows_addressable = MAX_ROWS - position as u64;
        let rows_removed = clamp(
            update.rows_removed + update.rows_changed,
            rows_addressable);
        let rows_added = clamp(
            update.rows_added + update.rows_changed,
            rows_addressable);

        self.items_changed(
            position as u32,
            rows_removed as u32,
            rows_added as u32);

        Ok(())
    }
}
