use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Debug;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex, MutexGuard};
use std::ops::DerefMut;

use itertools::Itertools;
use thiserror::Error;

use crate::capture::{Capture, CaptureError, ItemSource};

#[derive(Error, Debug)]
pub enum ModelError {
    #[error(transparent)]
    CaptureError(#[from] CaptureError),
    #[error("Locking capture failed")]
    LockError,
    #[error("Node references a dropped parent")]
    ParentDropped,
    #[error("Node already in requested expansion state")]
    AlreadyDone,
    #[error("Internal error: {0}")]
    InternalError(String),
}

use ModelError::{LockError, ParentDropped, AlreadyDone, InternalError};

pub type ItemNodeRc<Item> = Rc<RefCell<ItemNode<Item>>>;
type AnyNodeRc<Item> = Rc<RefCell<dyn Node<Item>>>;

trait Node<Item> {
    /// Item at this node, or None if the root.
    fn item(&self) -> Option<Item>;

    /// Parent of this node, or None if the root.
    fn parent(&self) -> Result<Option<AnyNodeRc<Item>>, ModelError>;

    /// Access the expanded children of this node.
    fn children(&self) -> &Children<Item>;

    /// Mutably access the expanded children of this node.
    fn children_mut(&mut self) -> &mut Children<Item>;
}

struct Children<Item> {
    /// Number of direct children below this node.
    count: u64,

    /// Expanded children of this item.
    expanded: BTreeMap<u64, ItemNodeRc<Item>>,
}

impl<Item> Children<Item> {
    fn new(child_count: u64) -> Self {
        Children {
            count: child_count,
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
    item_index: u64,

    /// Children of this item.
    children: Children<Item>,
}

impl<Item> Children<Item> {
    /// Whether this child is expanded.
    fn expanded(&self, node: &ItemNode<Item>) -> bool {
        self.expanded.contains_key(&node.item_index)
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
    fn item(&self) -> Option<Item> {
        None
    }

    fn parent(&self) -> Result<Option<AnyNodeRc<Item>>, ModelError> {
        Ok(None)
    }

    fn children(&self) -> &Children<Item> {
        &self.children
    }

    fn children_mut(&mut self) -> &mut Children<Item> {
        &mut self.children
    }
}

impl<Item> Node<Item> for ItemNode<Item> where Item: Copy {
    fn item(&self) -> Option<Item> {
        Some(self.item)
    }

    fn parent(&self) -> Result<Option<AnyNodeRc<Item>>, ModelError> {
        Ok(Some(self.parent.upgrade().ok_or(ModelError::ParentDropped)?))
    }

    fn children(&self) -> &Children<Item> {
        &self.children
    }

    fn children_mut(&mut self) -> &mut Children<Item> {
        &mut self.children
    }
}

impl<Item> ItemNode<Item> where Item: Copy + 'static {
    fn parent_is(&self, node_ref: &ItemNodeRc<Item>)
        -> Result<bool, ModelError>
    {
        let parent_ref = self.parent.upgrade().ok_or(ParentDropped)?;
        let node_ref: AnyNodeRc<Item> = node_ref.clone();
        Ok(Rc::ptr_eq(&parent_ref, &node_ref))
    }

    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_ref) => parent_ref
                .borrow()
                .children()
                .expanded(self),
            // Parent is dropped, so node cannot be expanded.
            None => false
        }
    }

    pub fn expandable(&self) -> bool {
        self.children.count != 0
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

#[derive(Clone)]
enum Source<Item> {
    TopLevelItems(),
    ChildrenOf(ItemNodeRc<Item>),
}

use Source::*;

#[derive(Clone)]
struct Region<Item> {
    source: Source<Item>,
    offset: u64,
    length: u64,
}

impl<Item> Debug for Region<Item>
where Item: Clone + Debug
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>)
        -> Result<(), std::fmt::Error>
    {
        use Source::*;
        match &self.source {
            TopLevelItems() =>
                write!(f, "Top level items"),
            ChildrenOf(rc) =>
                write!(f, "Children of {:?}", rc.borrow().item),
        }?;
        write!(f, ", offset {}, length {}", self.offset, self.length)
    }
}

impl<Item> Region<Item> where Item: Clone {
    fn merge(
        region_a: &Region<Item>,
        region_b: &Region<Item>
    ) -> Option<Region<Item>> {
        match (&region_a.source, &region_b.source) {
            (ChildrenOf(a_ref), ChildrenOf(b_ref))
                if Rc::ptr_eq(a_ref, b_ref) => Some(
                    Region {
                        source: region_a.source.clone(),
                        offset: region_a.offset,
                        length: region_a.length + region_b.length,
                    }
                ),
            (TopLevelItems(), TopLevelItems()) => Some(
                Region {
                    source: TopLevelItems(),
                    offset: region_a.offset,
                    length: region_a.length + region_b.length,
                }
            ),
            (..) => None,
        }
    }
}

pub struct ModelUpdate {
    pub rows_added: u64,
    pub rows_removed: u64,
    pub rows_changed: u64,
}

impl Debug for ModelUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>)
        -> Result<(), std::fmt::Error>
    {
        write!(f, "{} added, {} removed, {} changed",
            self.rows_added, self.rows_removed, self.rows_changed)
    }
}

pub struct TreeListModel<Item> {
    capture: Arc<Mutex<Capture>>,
    regions: BTreeMap<u64, Region<Item>>,
    root: Rc<RefCell<RootNode<Item>>>,
    total_rows: u64,
}

impl<Item> TreeListModel<Item>
where Item: 'static + Copy + Debug,
      Capture: ItemSource<Item>
{
    pub fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        let mut cap = capture.lock().or(Err(ModelError::LockError))?;
        let item_count = cap.item_count(&None)?;
        let mut model = TreeListModel {
            capture: capture.clone(),
            regions: BTreeMap::new(),
            root: Rc::new(RefCell::new(RootNode {
                children: Children::new(item_count),
            })),
            total_rows: item_count,
        };
        model.regions.insert(0, Region {
            source: Source::TopLevelItems(),
            offset: 0,
            length: item_count,
        });
        Ok(model)
    }

    pub fn row_count(&self) -> u64 {
        self.total_rows
    }

    pub fn set_expanded(&mut self,
                        node_ref: &ItemNodeRc<Item>,
                        position: u64,
                        expanded: bool)
        -> Result<ModelUpdate, ModelError>
    {
        if node_ref.borrow().expanded() == expanded {
            return Err(AlreadyDone);
        }

        // New rows will be added or removed after the current one.
        let position = position + 1;

        // Update the region map.
        let update = if expanded {
            // Expand this node.
            self.expand(position, node_ref)?
        } else {
            // Collapse children of this node, from last to first.
            let mut child_rows_removed = 0;
            for (start, region) in self.regions
                .clone()
                .range(position..)
                .rev()
                .filter(|(_, region)| region.offset == 0)
            {
                if let ChildrenOf(child_ref) = &region.source {
                    if child_ref.borrow().parent_is(node_ref)? {
                        // The child is at the row before its region.
                        let child_pos = *start - 1;
                        let update =
                            self.set_expanded(child_ref, child_pos, false)?;
                        child_rows_removed += update.rows_removed;
                    }
                }
            }

            // Collapse this node.
            let mut update = self.collapse(position, node_ref)?;

            // Include results of collapsing children in rows removed.
            update.rows_removed += child_rows_removed;
            update
        };

        // Update the parent node's expanded children.
        let node = node_ref.borrow();
        let parent_ref = node.parent.upgrade().ok_or(ParentDropped)?;
        parent_ref
            .borrow_mut()
            .children_mut()
            .set_expanded(node_ref, expanded);

        #[cfg(feature="debug-region-map")]
        {
            println!();
            println!("Region map:");
            for (start, region) in self.regions.iter() {
                println!("{}: {:?}", start, region);
            }
        }

        Ok(update)
    }

    fn node(&self,
            mut cap: MutexGuard<'_, Capture>,
            parent_rc: &AnyNodeRc<Item>,
            index: u64,
            item: Item)
        -> Result<ItemNodeRc<Item>, ModelError>
    {
        let parent = parent_rc.borrow();
        Ok(match parent.children().expanded.get(&index) {
            // If this node is already expanded, use its existing Rc.
            Some(node_rc) => node_rc.clone(),
            // Otherwise, create a new node.
            None => {
                let child_count = cap.item_count(&Some(item))?;
                Rc::new(RefCell::new(ItemNode {
                    item,
                    parent: Rc::downgrade(parent_rc),
                    item_index: index,
                    children: Children {
                        count: child_count,
                        expanded: BTreeMap::new()
                    }
                }))
            }
        })
    }

    fn expand(&mut self,
              position: u64,
              node_ref: &ItemNodeRc<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        // Find the start of the parent region.
        let (&parent_start, _) = self.regions
            .range(..position)
            .next_back()
            .ok_or_else(||
                InternalError(format!(
                    "No region before position {}", position)))?;

        // Find position of the new region relative to its parent.
        let relative_position = position - parent_start;

        // Remove the parent region.
        let parent = self.regions
            .remove(&parent_start)
            .ok_or_else(||
                InternalError(format!(
                    "Parent not found at position {}", parent_start)))?;

        // Remove all following regions, to iterate over later.
        let following_regions = self.regions
            .split_off(&parent_start)
            .into_iter();

        // Split the parent region and construct a new region between.
        let update = self.split_parent(parent_start, &parent, node_ref,
            vec![Region {
                source: parent.source.clone(),
                offset: parent.offset,
                length: relative_position,
            }],
            Region {
                source: ChildrenOf(node_ref.clone()),
                offset: 0,
                length: node_ref.borrow().children.count,
            },
            vec![Region {
                source: parent.source.clone(),
                offset: parent.offset + relative_position,
                length: parent.length - relative_position,
            }]
        )?;

        // Shift all remaining regions down by the added rows.
        for (start, region) in following_regions {
            self.insert_region(start + update.rows_added, region)?;
        }

        // Merge adjacent regions with the same source.
        self.merge_regions();

        // Update total row count.
        self.total_rows += update.rows_added;

        Ok(update)
    }

    fn collapse(&mut self,
                position: u64,
                node_ref: &ItemNodeRc<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        // Clone the region starting at this position.
        let region = self.regions
            .get(&position)
            .ok_or_else(||
                InternalError(format!(
                    "No region to delete at position {}", position)))?
            .clone();

        // Remove it with following regions, to iterate over and replace them.
        let mut following_regions = self.regions
            .split_off(&position)
            .into_iter();

        // Process the effects of removing this region.
        let update = match &region.source {
            // Root regions cannot be collapsed.
            TopLevelItems() => return Err(InternalError(String::from(
                "Unable to collapse root region"))),
            // Non-interleaved region is just removed.
            ChildrenOf(_) => {
                let (_, _region) = following_regions.next().unwrap();
                #[cfg(feature="debug-region-map")]
                {
                    println!();
                    println!("Removing: {:?}", _region);
                }
                ModelUpdate {
                    rows_added: 0,
                    rows_removed: node_ref.borrow().children.count,
                    rows_changed: 0,
                }
            }
        };

        // Shift all following regions up by the removed rows.
        for (start, region) in following_regions {
            self.insert_region(start - update.rows_removed, region)?;
        }

        // Merge adjacent regions with the same source.
        self.merge_regions();

        // Update total row count.
        self.total_rows -= update.rows_removed;

        Ok(update)
    }

    fn insert_region(&mut self,
                     position: u64,
                     region: Region<Item>)
        -> Result<(), ModelError>
    {
        match self.regions.entry(position) {
            Entry::Occupied(mut entry) => {
                let old_region = entry.get();
                if old_region.length == 0 {
                    entry.insert(region);
                    Ok(())
                } else {
                    Err(InternalError(format!(
                        "At position {}, overwriting region", position)))
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(region);
                Ok(())
            }
        }
    }

    fn split_parent(&mut self,
                    parent_start: u64,
                    parent: &Region<Item>,
                    _node_ref: &ItemNodeRc<Item>,
                    parts_before: Vec<Region<Item>>,
                    new_region: Region<Item>,
                    parts_after: Vec<Region<Item>>)
        -> Result<ModelUpdate, ModelError>
    {
        let length_before: u64 = parts_before
            .iter()
            .map(|region| region.length)
            .sum();

        let length_after: u64 = parts_after
            .iter()
            .map(|region| region.length)
            .sum();

        let total_length = length_before + new_region.length + length_after;

        let rows_added = total_length - parent.length;
        let rows_changed = parent.length - length_before - length_after;

        let update = ModelUpdate {
            rows_added,
            rows_removed: 0,
            rows_changed,
        };

        #[cfg(feature="debug-region-map")]
        {
            println!();
            println!("Splitting: {:?}", parent);
            for region in parts_before.iter() {
                println!("   before: {:?}", region);
            }
            println!("      new: {:?}", new_region);
            for region in parts_after.iter().filter(|region| region.length > 0) {
                println!("    after: {:?}", region);
            }
            println!("           {:?}", update);
        }

        let new_position = parent_start + length_before;
        let position_after = new_position + new_region.length;

        let mut position = parent_start;
        for region in parts_before {
            let length = region.length;
            self.insert_region(position, region)?;
            position += length;
        }

        self.insert_region(new_position, new_region)?;

        position = position_after;
        for region in parts_after
            .into_iter()
            .filter(|region| region.length > 0)
        {
            let length = region.length;
            self.insert_region(position, region)?;
            position += length;
        }

        Ok(update)
    }

    fn merge_regions(&mut self) {
        #[cfg(feature="debug-region-map")]
        {
            println!();
            println!("Before merge:");
            for (start, region) in self.regions.iter() {
                println!("{}: {:?}", start, region);
            }
        }

        self.regions = self.regions
            .split_off(&0)
            .into_iter()
            .coalesce(|(start_a, region_a), (start_b, region_b)|
                match Region::merge(&region_a, &region_b) {
                    Some(region_c) => {
                        #[cfg(feature="debug-region-map")]
                        {
                            println!();
                            println!("Merging: {:?}", region_a);
                            println!("    and: {:?}", region_b);
                            println!("   into: {:?}", region_c);
                        }
                        Ok((start_a, region_c))
                    },
                    None => Err(((start_a, region_a), (start_b, region_b)))
                }
            )
            .collect();
    }

    pub fn update(&mut self) -> Result<Option<(u64, ModelUpdate)>, ModelError> {
        let mut cap = self.capture.lock().or(Err(ModelError::LockError))?;

        let mut root = self.root.borrow_mut();
        let new_item_count = cap.item_count(&None)?;
        let old_item_count = root.children.count;

        if new_item_count == old_item_count {
            return Ok(None);
        }

        let position = self.total_rows;
        let update = ModelUpdate {
            rows_added: new_item_count - old_item_count,
            rows_removed: 0,
            rows_changed: 0,
        };

        root.children.count = new_item_count;
        self.total_rows += update.rows_added;

        drop(root);
        drop(cap);

        // Update the region map.
        match self.regions.iter_mut().next_back() {
            None => {
                // The map is empty, these are the first items.
                self.insert_region(
                    0,
                    Region {
                        source: TopLevelItems(),
                        offset: 0,
                        length: update.rows_added,
                    }
                )?;
            },
            Some((_start, region)) => match region.source {
                TopLevelItems() =>
                    // Last region is top level items, extend it.
                    region.length += update.rows_added,
                ChildrenOf(_) => {
                    // Last region is children of the last item.
                    // Add a new region for the top level items that follow.
                    self.insert_region(
                        position,
                        Region {
                            source: TopLevelItems(),
                            offset: old_item_count,
                            length: update.rows_added,
                        }
                    )?
                }
            }
        };

        Ok(Some((position, update)))
    }

    pub fn fetch(&self, position: u64) -> Result<ItemNodeRc<Item>, ModelError> {
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
        Ok(match &region.source {
            // Region containing top level items.
            TopLevelItems() => {
                let item = cap.item(&None, index)?;
                let parent_rc: AnyNodeRc<Item> = self.root.clone();
                self.node(cap, &parent_rc, index, item)?
            },
            // Region containing children of a parent item.
            ChildrenOf(parent_rc) => {
                let parent_item = parent_rc.borrow().item;
                let item = cap.child_item(&parent_item, index)?;
                let parent_rc: AnyNodeRc<Item> = parent_rc.clone();
                self.node(cap, &parent_rc, index, item)?
            },
        })
    }
}
