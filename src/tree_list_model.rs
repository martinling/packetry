use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::mem::drop;
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

pub type ItemNodeRc<Item> = Rc<RefCell<ItemNode<Item>>>;
type AnyNodeRc<Item> = Rc<RefCell<dyn Node<Item>>>;

trait Node<Item> {
    /// Item at this node, or None if the root.
    fn item(&self) -> Option<&Item>;

    /// Parent of this node, or None if the root.
    fn parent(&self) -> Result<Option<AnyNodeRc<Item>>, ModelError>;

    /// Position of this node in a list, relative to its parent node.
    fn relative_position(&self) -> Result<u32, ModelError>;

    /// Access the children of this node.
    fn children(&self) -> &Children<Item>;

    /// Mutably access the children of this node.
    fn children_mut(&mut self) -> &mut Children<Item>;
}

struct Children<Item> {
    /// Number of direct children below this node.
    direct_count: u32,

    /// Total number of displayed rows below this node, recursively.
    total_count: u32,

    /// Expanded children of this item.
    expanded: BTreeMap<u32, ItemNodeRc<Item>>,
}

impl<Item> Children<Item> {
    fn new(child_count: u32) -> Self {
        Children {
            direct_count: child_count,
            total_count: child_count,
            expanded: BTreeMap::new()
        }
    }
}

struct RootNode<Item> {
    /// Top level children.
    children: Children<Item>,
}

pub struct ItemNode<Item> {
    /// The item at this tree node.
    item: Item,

    /// Parent of this node in the tree.
    parent: Weak<RefCell<dyn Node<Item>>>,

    /// Index of this node below the parent Item.
    item_index: u32,

    /// Children of this item.
    children: Children<Item>,
}

impl<Item> Children<Item> {
    /// Whether this child is expanded.
    fn expanded(&self, index: u32) -> bool {
        self.expanded.contains_key(&index)
    }

    /// Iterate over the expanded children.
    fn iter_expanded(&self) -> impl Iterator<Item=(&u32, &ItemNodeRc<Item>)> + '_ {
        self.expanded.iter()
    }

    /// Set whether this child of the owning node is expanded.
    fn set_expanded(&mut self, child_rc: &ItemNodeRc<Item>, expanded: bool) {
        let child = child_rc.borrow();
        if expanded {
            self.expanded.insert(child.item_index, child_rc.clone());
        } else {
            self.expanded.remove(&child.item_index);
        }
    }
}

impl<Item> Node<Item> for RootNode<Item> {
    fn item(&self) -> Option<&Item> {
        None
    }

    fn parent(&self) -> Result<Option<AnyNodeRc<Item>>, ModelError> {
        Ok(None)
    }

    fn relative_position(&self) -> Result<u32, ModelError> {
        Ok(0)
    }

    fn children(&self) -> &Children<Item> {
        &self.children
    }

    fn children_mut(&mut self) -> &mut Children<Item> {
        &mut self.children
    }
}

impl<Item> Node<Item> for ItemNode<Item> where Item: Copy {
    fn item(&self) -> Option<&Item> {
        Some(&self.item)
    }

    fn parent(&self) -> Result<Option<AnyNodeRc<Item>>, ModelError> {
        Ok(Some(self.parent.upgrade().ok_or(ModelError::ParentDropped)?))
    }

    fn relative_position(&self) -> Result<u32, ModelError> {
        let parent = self.parent.upgrade()
            .ok_or(ModelError::ParentDropped)?;
        // Sum up the child counts of any expanded nodes before this one,
        // and add to `item_index`.
        let position = parent
            .borrow()
            .children()
            .iter_expanded()
            .take_while(|(&key, _)| key < self.item_index)
            .map(|(_, node)| node.borrow().children.total_count)
            .sum::<u32>() + self.item_index;
        Ok(position)
    }

    fn children(&self) -> &Children<Item> {
        &self.children
    }

    fn children_mut(&mut self) -> &mut Children<Item> {
        &mut self.children
    }
}

impl<Item> ItemNode<Item> where Item: Copy {
    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_ref) => parent_ref
                .borrow()
                .children()
                .expanded(self.item_index),
            // Parent is dropped, so node cannot be expanded.
            None => false
        }
    }

    pub fn expandable(&self) -> bool {
        self.children.total_count != 0
    }

    #[allow(clippy::type_complexity)]
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

impl<Item, Model, RowData> TreeListModel<Item, Model, RowData>
where Item: 'static + Copy,
      Model: GenericModel<Item> + ListModelExt,
      RowData: GenericRowData<Item> + IsA<Object> + Cast,
      Capture: ItemSource<Item>
{
    pub fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        let mut cap = capture.lock().or(Err(ModelError::LockError))?;
        let (_completion, item_count) = cap.item_children(None)?;
        let child_count = u32::try_from(item_count)?;
        Ok(TreeListModel {
            _marker: PhantomData,
            capture: capture.clone(),
            root: Rc::new(RefCell::new(RootNode {
                children: Children::new(child_count),
            })),
        })
    }

    pub fn set_expanded(&self,
                        model: &Model,
                        node_ref: &ItemNodeRc<Item>,
                        expanded: bool)
        -> Result<(), ModelError>
    {
        let node = node_ref.borrow();
        let mut position = node.relative_position()?;
        if node.expanded() == expanded {
            return Ok(());
        }

        let parent_rc = node.parent
            .upgrade()
            .ok_or(ModelError::ParentDropped)?;

        let rows_affected = node.children.direct_count;
        let expanded_children = node.children.expanded.clone();

        drop(node);

        // If collapsing, first recursively collapse the children of this node.
        if !expanded {
            for (_index, child_ref) in expanded_children {
                self.set_expanded(model, &child_ref, false)?;
            }
        }

        // Add or remove this node from the parent's expanded children.
        parent_rc
            .borrow_mut()
            .children_mut()
            .set_expanded(node_ref, expanded);

        // Traverse back up the tree, modifying `children.total_count` for
        // expanded/collapsed entries.
        let mut current_node: AnyNodeRc<Item> = node_ref.clone();
        while let Some(parent_ref) = current_node.clone().borrow().parent()? {
            let mut parent = parent_ref.borrow_mut();
            let children = parent.children_mut();
            if expanded {
                children.total_count += rows_affected;
            } else {
                children.total_count -= rows_affected
            }
            drop(parent);
            current_node = parent_ref;
            position += current_node.borrow().relative_position()? + 1;
        }

        if expanded {
            model.items_changed(position, 0, rows_affected);
        } else {
            model.items_changed(position, rows_affected, 0);
        }

        Ok(())
    }

    pub fn update(&self, model: &Model) -> Result<(), ModelError> {
        let mut cap = self.capture.lock().or(Err(ModelError::LockError))?;

        let mut root = self.root.borrow_mut();
        let (_completion, new_item_count) = cap.item_children(None)?;
        let new_item_count = new_item_count as u32;
        let old_item_count = root.children.direct_count;

        drop(cap);

        if new_item_count == old_item_count {
            return Ok(());
        }

        let position = root.children.total_count;
        let items_added = new_item_count - old_item_count;
        root.children.direct_count = new_item_count;
        root.children.total_count += items_added;

        drop(root);

        model.items_changed(position, 0, items_added);
        Ok(())
    }

    fn fetch(&self, position: u32) -> Result<ItemNodeRc<Item>, ModelError> {
        let mut parent_ref: Rc<RefCell<dyn Node<Item>>> = self.root.clone();
        let mut relative_position = position;
        'outer: loop {
            for (_, node_rc) in parent_ref
                .clone()
                .borrow()
                .children()
                .iter_expanded()
            {
                let node = node_rc.borrow();
                // If the position is before this node, break out of the loop to look it up.
                if relative_position < node.item_index {
                    break;
                // If the position matches this node, return it.
                } else if relative_position == node.item_index {
                    return Ok(node_rc.clone());
                // If the position is within this node's children, traverse down the tree and repeat.
                } else if relative_position <= node.item_index + node.children.total_count {
                    parent_ref = node_rc.clone();
                    relative_position -= node.item_index + 1;
                    continue 'outer;
                // Otherwise, if the position is after this node,
                // adjust the relative position for the node's children above.
                } else {
                    relative_position -= node.children.total_count;
                }
            }
            break;
        }

        // If we've broken out to this point, the node must be directly below `parent` - look it up.
        let mut cap = self.capture.lock().or(Err(ModelError::LockError))?;
        let parent = parent_ref.borrow();
        let item = cap.item(parent.item(), relative_position as u64)?;
        let (_completion, child_count) = cap.item_children(Some(&item))?;
        let node = ItemNode {
            item,
            parent: Rc::downgrade(&parent_ref),
            item_index: relative_position,
            children: Children::new(child_count.try_into()?),
        };

        Ok(Rc::new(RefCell::new(node)))
    }

    // The following methods correspond to the ListModel interface, and can be
    // called by a GObject wrapper class to implement that interface.

    pub fn n_items(&self) -> u32 {
        self.root.borrow().children.total_count
    }

    pub fn item(&self, position: u32) -> Option<Object> {
        // First check that the position is valid (must be within the root
        // node's total child count).
        if position >= self.root.borrow().children.total_count {
            return None
        }
        let node_or_err_msg = self.fetch(position).map_err(|e| format!("{:?}", e));
        let row_data = RowData::new(node_or_err_msg);
        Some(row_data.upcast::<Object>())
    }
}
