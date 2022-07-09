use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::num::TryFromIntError;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;

use gtk::prelude::{IsA, Cast};
use gtk::glib::Object;
use gtk::gio::prelude::ListModelExt;

use thiserror::Error;

use crate::capture::{Capture, CaptureError, ItemSource};
use crate::model::GenericModel;
use crate::row_data::GenericRowData;

#[derive(Error, Debug)]
pub enum ModelError {
    #[error(transparent)]
    CaptureError(#[from] CaptureError),
    #[error(transparent)]
    RangeError(#[from] TryFromIntError),
    #[error("Locking capture failed")]
    LockError,
    #[error("Node references a dropped parent")]
    ParentDropped,
}

pub trait Node<Item> {
    /// Item at this node, if not the root.
    fn item(&self) -> Option<Item>;

    /// Iterator over this node's expanded children.
    fn children(&self)
        -> Box<dyn Iterator<Item=(u32, &Rc<RefCell<TreeNode<Item>>>)> + '_>;

    /// Whether this node has an expanded child at this index.
    fn has_child(&self, index: u32) -> bool;

    /// Number of rows before the child with this index.
    fn rows_before(&self, index: u32) -> u32;

    /// Set whether the this child of the node is expanded.
    ///
    /// Returns the global position at which this occured.
    fn set_expanded(&mut self,
                    node_ref: &Rc<RefCell<TreeNode<Item>>>,
                    expanded: bool)
        -> Result<u32, ModelError>;

    /// Propagate a node being expanded or collapsed beneath this one.
    ///
    /// Takes the number of children added, the position within this
    /// node's children, and the position within the child's rows.
    ///
    /// Returns the global position of the change.
    fn propagate_expanded(&mut self,
                          count: u32,
                          index: u32,
                          position: u32,
                          expanded: bool)
        -> Result<u32, ModelError>;
}

pub struct RootNode<Item> {
    /// Total count of nodes in the tree.
    ///
    /// Initially this is set to the number of direct descendants,
    /// then increased/decreased as nodes are expanded/collapsed.
    child_count: u32,

    /// List of expanded child nodes directly below the root.
    children: BTreeMap<u32, Rc<RefCell<TreeNode<Item>>>>,
}

pub struct TreeNode<Item> {
    /// The item at this tree node.
    item: Item,

    /// Parent of this node in the tree.
    parent: Weak<RefCell<dyn Node<Item>>>,

    /// Index of this node below the parent Item.
    item_index: u32,

    /// Total count of nodes below this node, recursively.
    ///
    /// Initially this is set to the number of direct descendants,
    /// then increased/decreased as nodes are expanded/collapsed.
    child_count: u32,

    /// List of expanded child nodes directly below this node.
    children: BTreeMap<u32, Rc<RefCell<TreeNode<Item>>>>,
}

impl<Item> Node<Item> for RootNode<Item> {
    fn item(&self) -> Option<Item> {
        None
    }

    fn children(&self)
        -> Box<dyn Iterator<Item=(u32, &Rc<RefCell<TreeNode<Item>>>)> + '_>
    {
        Box::new(self.children
            .iter()
            .map(|(&index, node)| (index, node))
        )
    }

    fn has_child(&self, index: u32) -> bool {
        self.children.contains_key(&index)
    }

    fn rows_before(&self, index: u32) -> u32 {
        self.children
            .iter()
            .take_while(|(&key, _)| key < index)
            .map(|(_, node)| node.borrow().child_count)
            .sum::<u32>() + index
    }

    fn set_expanded(&mut self,
                    node_ref: &Rc<RefCell<TreeNode<Item>>>,
                    expanded: bool)
        -> Result<u32, ModelError>
    {
        let node = node_ref.borrow();
        if expanded {
            self.children.insert(node.item_index, node_ref.clone());
        } else {
            self.children.remove(&node.item_index);
        }
        self.propagate_expanded(
            node.child_count,
            node.item_index,
            1,
            expanded)
    }

    fn propagate_expanded(&mut self,
                          count: u32,
                          index: u32,
                          position: u32,
                          expanded: bool)
        -> Result<u32, ModelError>
    {
        if expanded {
            self.child_count += count;
        } else {
            self.child_count -= count;
        }
        Ok(self.rows_before(index) + position)
    }
}

impl<Item> Node<Item> for TreeNode<Item>
where Item: Copy
{
    fn item(&self) -> Option<Item> {
        Some(self.item)
    }

    fn children(&self)
        -> Box<dyn Iterator<Item=(u32, &Rc<RefCell<TreeNode<Item>>>)> + '_>
    {
        Box::new(self.children
            .iter()
            .map(|(&index, node)| (index, node))
        )
    }

    fn has_child(&self, index: u32) -> bool {
        self.children.contains_key(&index)
    }

    fn rows_before(&self, index: u32) -> u32 {
        self.children
            .iter()
            .take_while(|(&key, _)| key < index)
            .map(|(_, node)| node.borrow().child_count)
            .sum::<u32>() + index
    }

    fn set_expanded(&mut self,
                    node_ref: &Rc<RefCell<TreeNode<Item>>>,
                    expanded: bool)
        -> Result<u32, ModelError>
    {
        let node = node_ref.borrow();
        if expanded {
            self.children.insert(node.item_index, node_ref.clone());
        } else {
            self.children.remove(&node.item_index);
        }
        self.propagate_expanded(
            node.child_count,
            node.item_index,
            1,
            expanded)
    }

    fn propagate_expanded(&mut self,
                          count: u32,
                          index: u32,
                          position: u32,
                          expanded: bool)
        -> Result<u32, ModelError>
    {
        let parent_ref =
            self.parent.upgrade().ok_or(ModelError::ParentDropped)?;
        let mut parent = parent_ref.borrow_mut();
        if expanded {
            self.child_count += count;
        } else {
            self.child_count -= count;
        }
        parent.propagate_expanded(
            count,
            self.item_index,
            self.rows_before(index) + position + 1,
            expanded)
    }
}

impl<Item> TreeNode<Item> where Item: Copy {
    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_ref) => {
                let parent = parent_ref.borrow();
                parent.has_child(self.item_index)
            },
            // Parent is dropped, so node cannot be expanded.
            None => false
        }
    }

    pub fn expandable(&self) -> bool {
        self.child_count != 0
    }

    pub fn field(&self,
             capture: &Arc<Mutex<Capture>>,
             func: Box<dyn
                Fn(&mut Capture, &Item)
                    -> Result<String, CaptureError>>)
        -> String
    {
        match capture.lock() {
            Err(_) => "Error: failed to lock capture".to_string(),
            Ok(mut guard) => {
                let cap = guard.deref_mut();
                match func(cap, &self.item) {
                    Err(e) => format!("Error: {:?}", e),
                    Ok(string) => string
                }
            }
        }
    }
}

pub struct TreeListModel<Item, Model, RowData> {
    _marker: PhantomData<(Model, RowData)>,
    capture: Arc<Mutex<Capture>>,
    root: Rc<RefCell<RootNode<Item>>>,
}

impl<Item: 'static, Model, RowData> TreeListModel<Item, Model, RowData>
where Item: Copy,
      Model: GenericModel<Item> + ListModelExt,
      RowData: GenericRowData<Item> + IsA<Object> + Cast,
      Capture: ItemSource<Item>
{
    pub fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        let mut cap = capture.lock().or(Err(ModelError::LockError))?;
        let item_count = cap.item_count(&None)?;
        Ok(TreeListModel {
            _marker: PhantomData,
            capture: capture.clone(),
            root: Rc::new(RefCell::new(RootNode {
                child_count: u32::try_from(item_count)?,
                children: Default::default(),
            })),
        })
    }

    pub fn set_expanded(&self,
                        model: &Model,
                        node_ref: &Rc<RefCell<TreeNode<Item>>>,
                        expanded: bool)
        -> Result<(), ModelError>
    {
        let node = node_ref.borrow();
        if node.expanded() == expanded {
            return Ok(());
        }

        let position = {
            let rc = node.parent.upgrade().ok_or(ModelError::ParentDropped)?;
            let mut parent = rc.borrow_mut();
            parent.set_expanded(node_ref, expanded)?
        };

        if expanded {
            model.items_changed(position, 0, node.child_count);
        } else {
            model.items_changed(position, node.child_count, 0);
        }

        Ok(())
    }

    // The following methods correspond to the ListModel interface, and can be
    // called by a GObject wrapper class to implement that interface.

    pub fn n_items(&self) -> u32 {
        self.root.borrow().child_count
    }

    pub fn item(&self, position: u32) -> Option<Object> {
        // First check that the position is valid (must be within the root node's `child_count`).
        let mut parent_ref: Rc<RefCell<dyn Node<Item>>> = self.root.clone();
        if position >= self.root.borrow().child_count {
            return None
        }

        let mut relative_position = position;
        'outer: loop {
            for (_, node_rc) in parent_ref.clone().borrow().children() {
                let node = node_rc.borrow();
                // If the position is before this node, break out of the loop to look it up.
                if relative_position < node.item_index {
                    break;
                // If the position matches this node, return it.
                } else if relative_position == node.item_index {
                    return Some(RowData::new(node_rc.clone()).upcast::<Object>());
                // If the position is within this node's children, traverse down the tree and repeat.
                } else if relative_position <= node.item_index + node.child_count {
                    parent_ref = node_rc.clone();
                    relative_position -= node.item_index + 1;
                    continue 'outer;
                // Otherwise, if the position is after this node,
                // adjust the relative position for the node's children above.
                } else {
                    relative_position -= node.child_count;
                }
            }
            break;
        }

        // If we've broken out to this point, the node must be directly below `parent` - look it up.
        let mut cap = self.capture.lock().ok()?;
        let parent = parent_ref.borrow();
        let item = cap.item(&parent.item(), relative_position as u64).ok()?;
        let child_count = cap.child_count(&item).ok()?;
        let node = TreeNode {
            item,
            parent: Rc::downgrade(&parent_ref),
            item_index: relative_position,
            child_count: child_count.try_into().ok()?,
            children: Default::default(),
        };
        let rowdata = RowData::new(Rc::new(RefCell::new(node)));

        Some(rowdata.upcast::<Object>())
    }
}
