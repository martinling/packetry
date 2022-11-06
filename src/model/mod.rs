//! Defines our custom model

mod imp;

use std::sync::{Arc, Mutex};

use gtk::prelude::ListModelExt;
use gtk::subclass::prelude::*;
use gtk::{gio, glib};

use crate::capture::{Capture, TrafficItem, DeviceItem};
use crate::tree_list_model::{TreeListModel, ItemNodeRc, ModelError};

// Public part of the Model type.
glib::wrapper! {
    pub struct TrafficModel(ObjectSubclass<imp::TrafficModel>) @implements gio::ListModel;
}
glib::wrapper! {
    pub struct DeviceModel(ObjectSubclass<imp::DeviceModel>) @implements gio::ListModel;
}

pub trait GenericModel<Item> where Self: Sized {
    fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError>;
    fn set_expanded(&self,
                    node: &ItemNodeRc<Item>,
                    position: u32,
                    expanded: bool)
        -> Result<(), ModelError>;
    fn update(&self) -> Result<(), ModelError>;
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
                    node: &ItemNodeRc<TrafficItem>,
                    position: u32,
                    expanded: bool)
        -> Result<(), ModelError>
    {
        let tree_opt  = self.imp().tree.borrow();
        let tree = tree_opt.as_ref().unwrap();
        let update = tree.set_expanded(node, position, expanded)?;
        let rows_removed = update.rows_removed + update.rows_changed;
        let rows_added = update.rows_added + update.rows_changed;
        self.items_changed(position + 1, rows_removed, rows_added);
        Ok(())
    }

    fn update(&self) -> Result<(), ModelError> {
        let mut tree_opt  = self.imp().tree.borrow_mut();
        let tree = tree_opt.as_mut().unwrap();
        if let Some((position, update)) = tree.update()? {
            let rows_removed = update.rows_removed + update.rows_changed;
            let rows_added = update.rows_added + update.rows_changed;
            self.items_changed(position, rows_removed, rows_added);
        }
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
                    node: &ItemNodeRc<DeviceItem>,
                    position: u32,
                    expanded: bool)
        -> Result<(), ModelError>
    {
        let tree_opt  = self.imp().tree.borrow();
        let tree = tree_opt.as_ref().unwrap();
        let update = tree.set_expanded(node, position, expanded)?;
        let rows_removed = update.rows_removed + update.rows_changed;
        let rows_added = update.rows_added + update.rows_changed;
        self.items_changed(position + 1, rows_removed, rows_added);
        Ok(())
    }

    fn update(&self) -> Result<(), ModelError> {
        let mut tree_opt  = self.imp().tree.borrow_mut();
        let tree = tree_opt.as_mut().unwrap();
        if let Some((position, update)) = tree.update()? {
            let rows_removed = update.rows_removed + update.rows_changed;
            let rows_added = update.rows_added + update.rows_changed;
            self.items_changed(position, rows_removed, rows_added);
        }
        Ok(())
    }
}
