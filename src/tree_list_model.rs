use std::cell::RefCell;
use std::collections::BTreeMap;
use std::cmp::max;
use std::marker::PhantomData;
use std::num::TryFromIntError;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;

use gtk::prelude::{IsA, Cast};
use gtk::glib::Object;
use gtk::gio::prelude::ListModelExt;

use rtrees::rbtree::{RBTree, Augment};
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

use ModelError::ParentDropped;

pub trait Node<Item> {
    /// Item at this node, if not the root.
    fn item(&self) -> Option<Item>;

    /// Iterator over this node's expanded children.
    fn expanded(&self)
        -> Box<dyn Iterator<Item=(u32, &Rc<RefCell<TreeNode<Item>>>)> + '_>;

    /// Whether this node has an expanded child at this index.
    fn has_expanded(&self, index: u32) -> bool;

    /// Get the expanded child node with this index.
    fn get_expanded(&self, index: u32)
        -> Option<&Rc<RefCell<TreeNode<Item>>>>;

    /// Total number of rows for this node.
    fn total_rows(&self) -> u32;

    /// Number of rows before the child with this index.
    fn rows_before(&self, index: u32) -> u32;

    /// Set whether the this child of the node is expanded.
    fn set_expanded(&mut self,
                    node_ref: &Rc<RefCell<TreeNode<Item>>>,
                    expanded: bool);

    /// Propagate a node being expanded or collapsed beneath this one.
    ///
    /// Takes the number of rows added/removed, and the index within this
    /// node's child items.
    fn propagate_expanded(&mut self,
                          row_count: u32,
                          item_index: u32,
                          expanded: bool);

    /// If this node is not the root, get the Rcs to it and its parent.
    fn next_refs(&self)
        -> Result<Option<(
            Rc<RefCell<TreeNode<Item>>>,
            Rc<RefCell<dyn Node<Item>>>)>,
        ModelError>;
}

pub struct RootNode<Item> {
    /// Number of top-level items.
    item_count: u32,

    /// Interval tree of expanded top level items.
    expanded: RBTree<Interval, AugData, Rc<RefCell<TreeNode<Item>>>>,
}

pub struct TreeNode<Item> {
    /// The item at this node.
    item: Item,

    /// The node holding the parent of this item.
    parent: Weak<RefCell<dyn Node<Item>>>,

    /// Index of this item below its parent item.
    item_index: u32,

    /// Number of child items below this item.
    item_count: u32,

    /// Total number of rows associated with this item.
    ///
    /// Initially this is set to the child count of this item,
    /// then increased/decreased as nodes are expanded/collapsed.
    row_count: u32,

    /// Expanded children of this item, by index.
    expanded: BTreeMap<u32, Rc<RefCell<TreeNode<Item>>>>,
}

// This can't just be Option<u32> because we need Incomplete ordered last.
#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
enum IntervalEnd {
    Complete(u32),
    Incomplete
}

#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Interval {
    start: u32,
    end: IntervalEnd,
}

#[derive(Copy, Clone)]
pub struct AugData {
    last_end: IntervalEnd,
    total_rows: u32,
}

impl<Item> Augment<AugData> for
    RBTree<Interval, AugData, Rc<RefCell<TreeNode<Item>>>>
{
    fn sync_custom_aug(&mut self) {
        if !self.is_node() {
            return;
        }
        let own_rows = self.data_ref().borrow().row_count;
        let own_end = self.key().end;
        let left = self.left_ref();
        let right = self.right_ref();
        let mut aug_data = AugData {
            total_rows: own_rows,
            last_end: own_end
        };
        if left.is_node() {
            let left = left.aug_data();
            aug_data.total_rows += left.total_rows;
            aug_data.last_end = max(aug_data.last_end, left.last_end);
        }
        if right.is_node() {
            let right = right.aug_data();
            aug_data.total_rows += right.total_rows;
            aug_data.last_end = max(aug_data.last_end, right.last_end);
        }
        self.set_aug_data(aug_data);
    }
}

impl<Item> Node<Item> for RootNode<Item> {
    fn item(&self) -> Option<Item> {
        None
    }

    fn expanded(&self)
        -> Box<dyn Iterator<Item=(u32, &Rc<RefCell<TreeNode<Item>>>)> + '_>
    {
        Box::new((&self.expanded)
            .into_iter()
            .map(|(interval, _, node)| (interval.start, node))
        )
    }

    fn has_expanded(&self, index: u32) -> bool {
        self.get_expanded(index).is_some()
    }

    fn get_expanded(&self, index: u32) -> Option<&Rc<RefCell<TreeNode<Item>>>> {
        let interval = Interval {
            start: index,
            end: IntervalEnd::Complete(index)
        };
        self.expanded.search(interval)
    }

    fn total_rows(&self) -> u32 {
        let child_rows =
            if self.expanded.is_node() {
                self.expanded.aug_data().total_rows
            } else {
                0
            };
        self.item_count + child_rows
    }

    fn rows_before(&self, index: u32) -> u32 {
        (&self.expanded)
            .into_iter()
            .take_while(|(interval, _, _)| interval.start < index)
            .map(|(_, _, node)| node.borrow().row_count)
            .sum::<u32>() + index
    }

    fn set_expanded(&mut self,
                    node_ref: &Rc<RefCell<TreeNode<Item>>>,
                    expanded: bool)
    {
        let node = node_ref.borrow();
        let interval = Interval {
            start: node.item_index,
            end: IntervalEnd::Complete(node.item_index)
        };
        let aug_data = AugData {
            total_rows: node.row_count,
            last_end: interval.end,
        };
        if expanded {
            self.expanded.insert(interval, aug_data, node_ref.clone());
        } else {
            self.expanded.delete(interval);
        }
    }

    fn propagate_expanded(&mut self,
                          _row_count: u32,
                          item_index: u32,
                          _expanded: bool)
    {
        let interval = Interval {
            start: item_index,
            end: IntervalEnd::Complete(item_index)
        };
        self.expanded.force_sync_aug(interval);
    }

    fn next_refs(&self)
        -> Result<Option<(
            Rc<RefCell<TreeNode<Item>>>,
            Rc<RefCell<dyn Node<Item>>>)>,
        ModelError>
    {
        Ok(None)
    }
}

impl<Item> Node<Item> for TreeNode<Item>
where Item: Copy
{
    fn item(&self) -> Option<Item> {
        Some(self.item)
    }

    fn expanded(&self)
        -> Box<dyn Iterator<Item=(u32, &Rc<RefCell<TreeNode<Item>>>)> + '_>
    {
        Box::new(self.expanded
            .iter()
            .map(|(&index, node)| (index, node))
        )
    }

    fn has_expanded(&self, index: u32) -> bool {
        self.expanded.contains_key(&index)
    }

    fn get_expanded(&self, index: u32) -> Option<&Rc<RefCell<TreeNode<Item>>>> {
        self.expanded.get(&index)
    }

    fn total_rows(&self) -> u32 {
        self.row_count
    }

    fn rows_before(&self, index: u32) -> u32 {
        self.expanded
            .iter()
            .take_while(|(&key, _)| key < index)
            .map(|(_, node)| node.borrow().row_count)
            .sum::<u32>() + index
    }

    fn set_expanded(&mut self,
                    node_ref: &Rc<RefCell<TreeNode<Item>>>,
                    expanded: bool)
    {
        let node = node_ref.borrow();
        if expanded {
            self.expanded.insert(node.item_index, node_ref.clone());
        } else {
            self.expanded.remove(&node.item_index);
        }
    }

    fn propagate_expanded(&mut self,
                          row_count: u32,
                          _item_index: u32,
                          expanded: bool)
    {
        if expanded {
            self.row_count += row_count;
        } else {
            self.row_count -= row_count;
        }
    }

    fn next_refs(&self)
        -> Result<Option<(
            Rc<RefCell<TreeNode<Item>>>,
            Rc<RefCell<dyn Node<Item>>>)>,
        ModelError>
    {
        let parent_ref = self.parent.upgrade().ok_or(ParentDropped)?;
        let refs = parent_ref
            .borrow()
            .get_expanded(self.item_index)
            .map(|self_ref| (self_ref.clone(), parent_ref.clone()));
        Ok(refs)
    }
}

impl<Item> TreeNode<Item> where Item: Copy {
    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_ref) => {
                let parent = parent_ref.borrow();
                parent.has_expanded(self.item_index)
            },
            // Parent is dropped, so node cannot be expanded.
            None => false
        }
    }

    pub fn expandable(&self) -> bool {
        self.row_count != 0
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
        let item_count = u32::try_from(cap.item_count(&None)?)?;
        Ok(TreeListModel {
            _marker: PhantomData,
            capture: capture.clone(),
            root: Rc::new(RefCell::new(RootNode {
                item_count,
                expanded: RBTree::new(),
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
        let row_count = node.row_count;
        let mut current_ref = node_ref.clone();
        let mut parent_ref = node.parent.upgrade().ok_or(ParentDropped)?;
        let mut row_index = 0;
        parent_ref.borrow_mut().set_expanded(node_ref, expanded);
        while let Some((next_ref, next_parent_ref)) = {
            let item_index = current_ref.borrow().item_index;
            let mut parent = parent_ref.borrow_mut();
            parent.propagate_expanded(row_count, item_index, expanded);
            row_index += parent.rows_before(item_index) + 1;
            parent.next_refs()?
        } {
            current_ref = next_ref;
            parent_ref = next_parent_ref;
        }

        if expanded {
            model.items_changed(row_index, 0, row_count);
        } else {
            model.items_changed(row_index, row_count, 0);
        }

        Ok(())
    }

    // The following methods correspond to the ListModel interface, and can be
    // called by a GObject wrapper class to implement that interface.

    pub fn n_items(&self) -> u32 {
        self.root.borrow().total_rows()
    }

    pub fn item(&self, position: u32) -> Option<Object> {
        // First check that the position is valid.
        if position >= self.n_items() {
            return None
        }

        let mut parent_ref: Rc<RefCell<dyn Node<Item>>> = self.root.clone();
        let mut relative_position = position;
        'outer: loop {
            for (_, node_rc) in parent_ref.clone().borrow().expanded() {
                let node = node_rc.borrow();
                // If the position is before this node, break out of the loop to look it up.
                if relative_position < node.item_index {
                    break;
                // If the position matches this node, return it.
                } else if relative_position == node.item_index {
                    return Some(RowData::new(node_rc.clone()).upcast::<Object>());
                // If the position is within this node's children, traverse down the tree and repeat.
                } else if relative_position <= node.item_index + node.row_count {
                    parent_ref = node_rc.clone();
                    relative_position -= node.item_index + 1;
                    continue 'outer;
                // Otherwise, if the position is after this node,
                // adjust the relative position for the node's children above.
                } else {
                    relative_position -= node.row_count;
                }
            }
            break;
        }

        // If we've broken out to this point, the node must be directly below `parent` - look it up.
        let mut cap = self.capture.lock().ok()?;
        let parent = parent_ref.borrow();
        let item = cap.item(&parent.item(), relative_position as u64).ok()?;
        let item_count = u32::try_from(cap.child_count(&item).ok()?).ok()?;
        let node = TreeNode {
            item,
            parent: Rc::downgrade(&parent_ref),
            item_index: relative_position,
            item_count,
            row_count: item_count,
            expanded: Default::default(),
        };
        let rowdata = RowData::new(Rc::new(RefCell::new(node)));

        Some(rowdata.upcast::<Object>())
    }
}
