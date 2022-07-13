use std::cell::RefCell;
use std::collections::BTreeMap;
use std::cmp::{max, min};
use std::marker::PhantomData;
use std::num::TryFromIntError;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex, MutexGuard};
use std::ops::{DerefMut, Range};

use gtk::prelude::{IsA, Cast};
use gtk::glib::Object;
use rtrees::rbtree::{RBTree, Augment};
use thiserror::Error;

use crate::capture::{Capture, CaptureError, ItemSource, SearchResult};
use crate::id::HasLength;
use crate::interval::{Interval, IntervalEnd};
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
    #[error("Node already in requested expansion state")]
    AlreadyDone,
    #[error("Internal error: {0}")]
    InternalError(String),
}

use ModelError::{ParentDropped, LockError, AlreadyDone, InternalError};

pub type ItemRc<Item> = Rc<RefCell<ItemNode<Item>>>;
pub type NodeRc<Item> = Rc<RefCell<dyn Node<Item>>>;

pub trait Node<Item> {
    /// Item at this node, if not the root.
    fn item(&self) -> Option<Item>;

    /// Whether this node has an expanded child at this index.
    fn has_expanded(&self, index: u64) -> bool;

    /// Get the expanded child node with this index.
    fn get_expanded(&self, index: u64) -> Option<ItemRc<Item>>;

    /// Set whether the this child of the node is expanded.
    fn set_expanded(&mut self,
                    node_ref: &ItemRc<Item>,
                    expanded: bool);
}

pub struct RootNode<Item> {
    /// Interval tree of expanded top level items.
    expanded: RBTree<u64, AugData, ItemRc<Item>>,
}

pub struct ItemNode<Item> {
    /// The item at this node.
    item: Item,

    /// The node holding the parent of this item.
    parent: Weak<RefCell<dyn Node<Item>>>,

    /// Interval spanned by this item.
    interval: Interval,

    /// Number of children of this item.
    child_count: u64,

    /// Expanded children of this item, by index.
    expanded: BTreeMap<u64, ItemRc<Item>>,
}

#[derive(Copy, Clone)]
pub struct AugData {
    last_end: IntervalEnd,
}

impl<Item> Augment<AugData> for
    RBTree<u64, AugData, ItemRc<Item>>
{
    fn sync_custom_aug(&mut self) {
        if !self.is_node() {
            return;
        }
        let own_end = self.data_ref().borrow().interval.end;
        let left = self.left_ref();
        let right = self.right_ref();
        let mut aug_data = AugData {
            last_end: own_end
        };
        if left.is_node() {
            let left = left.aug_data();
            aug_data.last_end = max(aug_data.last_end, left.last_end);
        }
        if right.is_node() {
            let right = right.aug_data();
            aug_data.last_end = max(aug_data.last_end, right.last_end);
        }
        self.set_aug_data(aug_data);
    }
}

impl<Item> Node<Item> for RootNode<Item> {
    fn item(&self) -> Option<Item> {
        None
    }

    fn has_expanded(&self, index: u64) -> bool {
        self.expanded.search(index).is_some()
    }

    fn get_expanded(&self, index: u64) -> Option<ItemRc<Item>> {
        self.expanded.search(index).map(Rc::clone)
    }

    fn set_expanded(&mut self,
                    node_ref: &ItemRc<Item>,
                    expanded: bool)
    {
        let node = node_ref.borrow();
        let start = node.interval.start;
        let aug_data = AugData {
            last_end: node.interval.end,
        };
        if expanded {
            self.expanded.insert(start, aug_data, node_ref.clone());
        } else {
            self.expanded.delete(start);
        }
    }
}

impl<Item> Node<Item> for ItemNode<Item>
where Item: Copy
{
    fn item(&self) -> Option<Item> {
        Some(self.item)
    }

    fn has_expanded(&self, index: u64) -> bool {
        self.expanded.contains_key(&index)
    }

    fn get_expanded(&self, index: u64) -> Option<ItemRc<Item>> {
        self.expanded.get(&index).map(Rc::clone)
    }

    fn set_expanded(&mut self,
                    node_ref: &ItemRc<Item>,
                    expanded: bool)
    {
        let node = node_ref.borrow();
        let start = node.interval.start;
        if expanded {
            self.expanded.insert(start, node_ref.clone());
        } else {
            self.expanded.remove(&start);
        }
    }
}

impl<Item> ItemNode<Item> where Item: Copy {
    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_ref) => {
                let parent = parent_ref.borrow();
                parent.has_expanded(self.interval.start)
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

#[derive(Clone)]
pub enum Source<Item> {
    Children(NodeRc<Item>),
    Interleaved(Vec<ItemRc<Item>>, Range<u64>),
}

#[derive(Clone)]
pub struct Region<Item> {
    source: Rc<Source<Item>>,
    offset: u64
}

pub struct ModelUpdate {
    pub position: u32,
    pub rows_removed: u32,
    pub rows_added: u32,
}

pub struct TreeListModel<Item, RowData> {
    _marker: PhantomData<RowData>,
    capture: Arc<Mutex<Capture>>,
    regions: BTreeMap<u64, Region<Item>>,
    root: Rc<RefCell<RootNode<Item>>>,
    item_count: u64,
    row_count: u64,
}

const MAX_ROWS: u64 = u32::MAX as u64;

fn clamp(value: u64, maximum: u64) -> u32 {
    min(value, maximum) as u32
}

impl<Item: 'static, RowData> TreeListModel<Item, RowData>
where Item: Copy,
      RowData: GenericRowData<Item> + IsA<Object> + Cast,
      Capture: ItemSource<Item>
{
    pub fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        use Source::*;
        let mut cap = capture.lock().or(Err(LockError))?;
        let item_count = cap.item_count(&None)?;
        let mut model = TreeListModel {
            _marker: PhantomData,
            capture: capture.clone(),
            item_count,
            row_count: item_count,
            regions: BTreeMap::new(),
            root: Rc::new(RefCell::new(RootNode {
                expanded: RBTree::new(),
            })),
        };
        model.regions.insert(0,
            Region {
                source: Rc::new(Children(model.root.clone())),
                offset: 0
            });
        Ok(model)
    }

    pub fn node(&self,
                mut cap: MutexGuard<'_, Capture>,
                parent_rc: &NodeRc<Item>,
                index: u64,
                item: Item)
        -> Result<ItemRc<Item>, ModelError>
    {
        let parent = parent_rc.borrow();
        Ok(match parent.get_expanded(index) {
            // If this node is already expanded, use its existing Rc.
            Some(node_rc) => node_rc,
            // Otherwise, create a new node.
            None => {
                Rc::new(RefCell::new(ItemNode {
                    item,
                    parent: Rc::downgrade(parent_rc),
                    interval: Interval {
                        start: index,
                        end: cap.item_end(&item, index)?,
                    },
                    child_count: cap.child_count(&item)?,
                    expanded: Default::default(),
                }))
            }
        })
    }

    pub fn insert_region(&mut self,
                         position: u64,
                         node_ref: &ItemRc<Item>,
                         parent_source: &Rc<Source<Item>>,
                         offset: u64)
        -> Result<ModelUpdate, ModelError>
    {
        // Construct source for the new region.
        use IntervalEnd::*;
        use Source::*;
        let node = node_ref.borrow();
        let start = node.interval.start;
        let source = match node.interval.end {
            Incomplete =>
                Interleaved(
                    vec![node_ref.clone()],
                    start..self.item_count),
            Complete(end) if end > start =>
                Interleaved(
                    vec![node_ref.clone()],
                    start..end),
            Complete(_) =>
                Children(node_ref.clone()),
        };

        // Get number of rows added and replaced.
        let rows_added = node_ref.borrow().child_count;
        let rows_changed = match source {
            Interleaved(_, ref range) => range.len() - 1,
            Children(_) => 0,
        };

        // Split the current region if it has rows after this one.
        self.regions
            .entry(position)
            .or_insert_with(|| Region {
                source: parent_source.clone(),
                offset: offset + rows_changed,
            });

        // Shift all following regions down by the length of the new region.
        for (start, region) in self.regions.split_off(&position) {
            let region_length = rows_changed + rows_added;
            self.regions.insert(start + region_length, region);
        }

        // Insert new region.
        self.regions.insert(position,
            Region {
                source: Rc::new(source),
                offset: 0
            });

        // Update total row count.
        self.row_count += rows_added;

        // Clamp effects to u32 rows and return changes.
        let rows_addressable = MAX_ROWS - position;
        Ok(ModelUpdate {
            position: clamp(position, MAX_ROWS),
            rows_removed: clamp(rows_changed, rows_addressable),
            rows_added: clamp(rows_changed + rows_added, rows_addressable),
        })
    }

    pub fn delete_region(&mut self,
                         position: u64,
                         node_ref: &ItemRc<Item>,
                         parent_source: &Rc<Source<Item>>)
        -> Result<ModelUpdate, ModelError>
    {
        // Remove the region starting after the current row.
        let region = self.regions.remove(&position).ok_or_else(||
            InternalError(format!(
                "No region to delete at position {}", position)))?;

        // Get number of rows removed and replaced.
        use Source::*;
        let rows_removed = node_ref.borrow().child_count;
        let rows_changed = match region.source.as_ref() {
            Interleaved(_, range) => range.len() - 1,
            Children(_) => 0,
        };

        // Shift all following regions up by the length of the removed region.
        for (start, region) in self.regions.split_off(&position) {
            let region_length = rows_changed + rows_removed;
            self.regions.insert(start - region_length, region);
        }

        // Merge the preceding and following regions if they have the
        // same source.
        if let Some(next_region) = self.regions.get(&position) {
            if Rc::ptr_eq(parent_source, &next_region.source) {
                self.regions.remove(&position);
            }
        }

        // Update total row count.
        self.row_count -= rows_removed;

        // Clamp effects to u32 rows and return changes.
        let rows_addressable = MAX_ROWS - position;
        Ok(ModelUpdate {
            position: clamp(position, MAX_ROWS),
            rows_removed: clamp(rows_changed + rows_removed, rows_addressable),
            rows_added: clamp(rows_changed, rows_addressable),
        })
    }

    pub fn set_expanded(&mut self,
                        node_ref: &ItemRc<Item>,
                        position: u64,
                        expanded: bool)
        -> Result<ModelUpdate, ModelError>
    {
        if node_ref.borrow().expanded() == expanded {
            return Err(AlreadyDone);
        }

        // Fetch the region this row appears in and clone its source.
        let (start, region) =
            self.regions.range(..=position).next_back().ok_or_else(||
                InternalError(format!(
                    "No region before position {}", position)))?;
        let source = region.source.clone();

        // New rows will be added or removed after the current one.
        let position = position + 1;

        // Update the region map.
        let update = if expanded {
            // Insert a new region for this node.
            let offset = region.offset + position - start;
            self.insert_region(position, node_ref, &source, offset)?
        } else {
            // Collapse the children of this node and count the rows removed.
            let children = node_ref.borrow().expanded.clone().into_iter();
            let mut child_rows_removed = 0;
            for (i, child_ref) in children {
                child_rows_removed +=
                    self.set_expanded(&child_ref, position + i, false)?
                        .rows_removed as u64;
            }

            // Delete the region associated with this node.
            let mut update =
                self.delete_region(position, node_ref, &source)?;

            // Include removal of collapsed child rows in the model update.
            let rows_removed = update.rows_removed as u64 + child_rows_removed;
            update.rows_removed = clamp(rows_removed, MAX_ROWS - position);
            update
        };

        // Update the parent node's expanded children.
        let node = node_ref.borrow();
        let parent_ref = node.parent.upgrade().ok_or(ParentDropped)?;
        parent_ref.borrow_mut().set_expanded(node_ref, expanded);

        Ok(update)
    }

    fn fetch(&self, position: u64) -> Result<ItemRc<Item>, ModelError> {
        // Fetch the region this row is in.
        let (start, region) = self.regions
            .range(..=position)
            .next_back()
            .ok_or_else(||
                InternalError(format!(
                    "No region before position {}", position)))?;
        // Get the index of this row relative to the start of that region.
        let index = region.offset + (position - start);
        // Get the node for this row, according to the type of region.
        let mut cap = self.capture.lock().or(Err(LockError))?;
        use Source::*;
        use SearchResult::*;
        Ok(match region.source.as_ref() {
            // Simple region containing children of a parent item.
            Children(parent_rc) => {
                let parent_item = parent_rc.borrow().item();
                let item = cap.item(&parent_item, index)?;
                self.node(cap, parent_rc, index, item)?
            },
            // Region requiring interleaved search.
            Interleaved(expanded_rcs, range) => {
                // Prepare an iterator of expanded intervals to pass to the
                // search, which only requires the start and item.
                let mut expanded = expanded_rcs.iter().map(|node_rc| {
                    let node = node_rc.borrow();
                    (node.interval.start, node.item)
                });

                // Run the interleaved search.
                let search_result =
                    cap.find_child(&mut expanded, range, index)?;

                // Return a node corresponding to the search result.
                match search_result {
                    // Search found a top level item.
                    TopLevelItem(index, item) => {
                        let root_rc: NodeRc<Item> = self.root.clone();
                        self.node(cap, &root_rc, index, item)?
                    },
                    // Search found a child of an expanded top level item.
                    NextLevelItem(parent_index, child_index, item) => {
                        // There must already be a node for its parent.
                        let parent_rc: NodeRc<Item> = self.root
                            .borrow()
                            .get_expanded(parent_index)
                            .ok_or(ParentDropped)?;
                        self.node(cap, &parent_rc, child_index, item)?
                    }
                }
            }
        })
    }

    // The following methods correspond to the ListModel interface, and can be
    // called by a GObject wrapper class to implement that interface.

    pub fn n_items(&self) -> u32 {
        clamp(self.row_count, MAX_ROWS)
    }

    pub fn item(&self, position: u32) -> Option<Object> {
        // Check row number is valid.
        if position >= self.n_items() {
            return None
        }
        let node = self.fetch(position as u64).ok()?;
        let rowdata = RowData::new(node);
        Some(rowdata.upcast::<Object>())
    }
}
