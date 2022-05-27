use std::sync::{Arc, Mutex};

use gtk::subclass::prelude::*;
use gtk::{gio, glib};

use crate::capture::{Capture, Item};

pub enum NodeType {
    NoneExpanded,
    FirstExpanded(TreeNode, u64, u64, TreeNode),
}

use NodeType::*;

#[derive(Copy, Clone)]
pub enum TreeNode {
    pub item: Option<Item>,
    pub node_type: NodeType,
}

impl TreeNode {
    pub fn get_node(cap: MutexGuard<Capture>, position: u64) -> TreeNode {
        match self.node_type {
            NoneExpanded => {
                let item = cap.get_item(self.item, position).unwrap();
                TreeNode {
                    item,
                    NoneExpanded
                };
            },
            FirstExpanded(node, first, last, next) => {
                if position <= first {
                    let item = cap.get_item(self.item, position).unwrap();
                    TreeNode {
                        item,
                        FirstExpanded(position, node)
                    }
                } else if position > first && position < last {
                    node.get_node(cap, position - first)
                } else {
                    next.get_node(cap, position - last)
                }
            }
        }
    }
}

glib::wrapper! {
    pub struct TreeListModel(ObjectSubclass<imp::TreeListModel>) @implements gio::ListModel;
}

impl TreeListModel {
    pub fn new(capture: Arc<Mutex<Capture>>) -> Self {
        let mut model: TreeListModel =
            glib::Object::new(&[]).expect("Failed to create TreeListModel");
        {
            let mut cap = capture.lock().unwrap();
            model.set_item_count(cap.item_count(&None).unwrap());
        }
        model.set_capture(capture);
        model
    }

    fn set_item_count(&mut self, count: u64) {
        self.imp().item_count.set(count.try_into().unwrap());
    }

    fn set_capture(&mut self, capture: Arc<Mutex<Capture>>) {
        self.imp().capture.replace(Some(capture));
    }
}

mod imp {
    use std::cell::{Cell, RefCell};
    use std::sync::{Arc, Mutex};

    use gtk::subclass::prelude::*;
    use gtk::{prelude::*, gio, glib};
    use thiserror::Error;

    use crate::capture::{Capture, CaptureError};
    use crate::row_data::RowData;

    use super::TreeNode;

    #[derive(Error, Debug)]
    pub enum ModelError {
        #[error(transparent)]
        CaptureError(#[from] CaptureError),
        #[error("Capture not set")]
        CaptureNotSet,
        #[error("Locking capture failed")]
        LockError,
    }

    #[derive(Default)]
    pub struct TreeListModel {
        pub(super) capture: RefCell<Option<Arc<Mutex<Capture>>>>,
        pub(super) item_count: Cell<u32>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for TreeListModel {
        const NAME: &'static str = "TreeListModel";
        type Type = super::TreeListModel;
        type Interfaces = (gio::ListModel,);
    }

    impl ObjectImpl for TreeListModel {}

    impl ListModelImpl for TreeListModel {
        fn item_type(&self, _list_model: &Self::Type) -> glib::Type {
            RowData::static_type()
        }

        fn n_items(&self, _list_model: &Self::Type) -> u32 {
            self.item_count.get()
        }

        fn item(&self, _list_model: &Self::Type, position: u32)
            -> Option<glib::Object>
        {
            let opt = self.capture.borrow();
            let mut cap = match opt.as_ref() {
                Some(mutex) => match mutex.lock() {
                    Ok(guard) => guard,
                    Err(_) => return None
                },
                None => return None
            };
            let node = self.root.get_node(position as u64);
            Some(RowData::new(node).upcast::<glib::Object>())
        }
    }
}
