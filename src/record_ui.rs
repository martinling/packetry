use std::fmt::Debug;
use std::fs::File;
use std::path::Path;
use serde::{Serialize, Deserialize};

#[cfg(feature="record-ui-test")]
use {
    std::cell::RefCell,
    std::rc::Rc,
    std::io::Write,
};

#[cfg(test)]
use {
    std::io::BufReader,
    serde_json::Deserializer,
};

use gtk::prelude::*;
use gtk::glib::Object;
use gtk::gio::ListModel;

use crate::model::GenericModel;
use crate::row_data::GenericRowData;

#[derive(Serialize, Deserialize)]
pub enum UIAction {
    Startup(),
    SetExpanded(u32, bool)
}

#[derive(Serialize, Deserialize)]
struct UIState {
    action: UIAction,
    items: Vec<String>
}

#[cfg(feature="record-ui-test")]
pub fn create_logfile(dir: &Path, name: &str) -> Rc<RefCell<File>> {
    Rc::new(RefCell::new(
        File::options()
            .write(true)
            .create(true)
            .open(
                dir.join(
                    Path::new(
                        &format!("{}-log.json", name))))
            .expect("Failed to open UI log file")))
}

#[cfg(feature="record-ui-test")]
pub fn log_action<Model, RowData, Item>(
    model: &Model,
    logfile: &Rc<RefCell<File>>,
    action: UIAction)
where
    Model: GenericModel<Item> + IsA<ListModel>,
    RowData: GenericRowData<Item> + IsA<Object>,
    Item: 'static + Copy + Debug
{
    let item_count = model.n_items();
    let mut state = UIState {
        action,
        items: Vec::with_capacity(item_count as usize),
    };
    for i in 0..item_count {
        state.items.push(
            format!("{:?}",
                model
                    .item(i)
                    .expect("Failed to load model item")
                    .downcast::<RowData>()
                    .expect("Model item was not RowData.")
                    .node()
                    .expect("RowData has no node")
                    .borrow()
                    .item()));
    }
    logfile
        .borrow_mut()
        .write_all(
            serde_json::to_string_pretty(&state)
                .expect("Failed to serialise UI state")
                .as_bytes())
        .expect("Failed to write to UI log");
}

#[cfg(test)]
pub fn check_replay<Model, RowData, Item>(
    model: &Model,
    logfile: &Path)
where
    Model: GenericModel<Item> + IsA<ListModel>,
    RowData: GenericRowData<Item> + IsA<Object>,
    Item: 'static + Copy + Debug
{
    let file = File::open(logfile).unwrap();
    let mut reader = BufReader::new(file);
    let deserializer = Deserializer::from_reader(&mut reader);
    for state in deserializer.into_iter::<UIState>() {
        let state = state.unwrap();
        use UIAction::*;
        match state.action {
            Startup() => {},
            SetExpanded(position, expanded) => {
                let node = model
                    .item(position)
                    .expect("Failed to load model item")
                    .downcast::<RowData>()
                    .expect("Model item was not RowData.")
                    .node()
                    .expect("RowData has no node");
                model.set_expanded(&node, position, expanded)
                    .expect("Failed to expand/collapse node");
            }
        };
        for i in 0..model.n_items() {
            let item = model
                .item(i)
                .expect("Failed to load model item")
                .downcast::<RowData>()
                .expect("Model item was not RowData.")
                .node()
                .expect("RowData has no node")
                .borrow()
                .item();
            assert!(format!("{:?}", item) == state.items[i as usize]);
        }
    }
}
