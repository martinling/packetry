use std::cell::RefCell;
use std::collections::BTreeMap;
use std::cmp::{max, min};
use std::fmt::Debug;
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
    /// Whether this node has an expanded child at this index.
    fn has_expanded(&self, index: u64) -> bool;

    /// Get the expanded child node with this index.
    fn get_expanded(&self, index: u64) -> Option<ItemRc<Item>>;

    /// Set whether this child of the node is expanded.
    fn set_expanded(&mut self, child_rc: &ItemRc<Item>, expanded: bool);
}

pub struct RootNode<Item> {
    /// Interval tree of expanded top level items.
    expanded: IntervalTree<Item>,
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

struct IntervalTree<Item>(RBTree<u64, AugData, ItemRc<Item>>);

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
    fn has_expanded(&self, index: u64) -> bool {
        self.expanded.0.search(index).is_some()
    }

    fn get_expanded(&self, index: u64) -> Option<ItemRc<Item>> {
        self.expanded.0.search(index).map(Rc::clone)
    }

    fn set_expanded(&mut self, child_rc: &ItemRc<Item>, expanded: bool) {
        let child = child_rc.borrow();
        let start = child.interval.start;
        let aug_data = AugData {
            last_end: child.interval.end,
        };
        if expanded {
            self.expanded.0.insert(start, aug_data, child_rc.clone());
        } else {
            self.expanded.0.delete(start);
        }
    }
}

impl<Item> Node<Item> for ItemNode<Item> {
    fn has_expanded(&self, index: u64) -> bool {
        self.expanded.contains_key(&index)
    }

    fn get_expanded(&self, index: u64) -> Option<ItemRc<Item>> {
        self.expanded.get(&index).map(Rc::clone)
    }

    fn set_expanded(&mut self, child_rc: &ItemRc<Item>, expanded: bool) {
        let child = child_rc.borrow();
        let start = child.interval.start;
        if expanded {
            self.expanded.insert(start, child_rc.clone());
        } else {
            self.expanded.remove(&start);
        }
    }
}

impl<Item> ItemNode<Item>
where Item: 'static
{
    pub fn parent_is(&self, node_ref: &ItemRc<Item>)
        -> Result<bool, ModelError>
    {
        let parent_ref = self.parent.upgrade().ok_or(ParentDropped)?;
        let node_ref: NodeRc<Item> = node_ref.clone();
        Ok(Rc::ptr_eq(&parent_ref, &node_ref))
    }

    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_rc) => {
                let parent = parent_rc.borrow();
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
    Children(ItemRc<Item>),
    Interleaved(Vec<ItemRc<Item>>, Range<u64>),
}

#[derive(Clone)]
pub struct Region<Item> {
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
            Children(rc) =>
                write!(f, "Children of {:?}", rc.borrow().item),
            Interleaved(expanded, range) =>
                write!(f, "Interleaved search in {:?} from {:?}",
                    range,
                    expanded
                        .iter()
                        .map(|rc| rc.borrow().item.clone())
                        .collect::<Vec<Item>>()),
        }?;
        write!(f, ", offset {}, length {}", self.offset, self.length)
    }
}

#[derive(Default)]
pub struct ModelUpdate {
    pub rows_added: u64,
    pub rows_removed: u64,
    pub rows_changed: u64,
}

pub struct TreeListModel<Item, RowData> {
    _marker: PhantomData<RowData>,
    capture: Arc<Mutex<Capture>>,
    regions: BTreeMap<u64, Region<Item>>,
    root: Rc<RefCell<RootNode<Item>>>,
    item_count: u64,
    row_count: u64,
}

impl<Item, RowData> TreeListModel<Item, RowData>
where Item: Copy + Debug + 'static,
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
                expanded: IntervalTree(RBTree::new()),
            })),
        };
        model.regions.insert(0,
            Region {
                source: Interleaved(vec![], 0..item_count),
                offset: 0,
                length: item_count,
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

    fn adapt_expanded<'exp>(&self, expanded_rcs: &'exp [ItemRc<Item>])
        -> impl Iterator<Item=(u64, Item)> + 'exp
    {
        expanded_rcs.iter().map(|node_rc| {
            let node = node_rc.borrow();
            (node.interval.start, node.item)
        })
    }

    fn count_rows_to(&self,
                     expanded: &[ItemRc<Item>],
                     range: &Range<u64>,
                     node_ref: &ItemRc<Item>,
                     offset: u64)
        -> Result<u64, ModelError>
    {
        use IntervalEnd::*;
        use SearchResult::*;
        let node = node_ref.borrow();
        let item_index = node.interval.start;
        let item = &node.item;
        let mut expanded = self.adapt_expanded(expanded);
        let mut cap = self.capture.lock().or(Err(LockError))?;
        let search_result = cap.find_child(&mut expanded, range, offset);
        let start = node.interval.start;
        let end = match node.interval.end {
            Incomplete => self.item_count,
            Complete(end) if end > start => end,
            _ => start,
        };
        let item_range = start..end;
        Ok(match search_result {
            Ok(NextLevelItem(span_index, .., child)) => {
                cap.count_within(item_index, item, &item_range)? +
                cap.count_before(item_index, item, span_index, &child)?
            },
            _ => cap.count_within(item_index, item, &item_range)?
        })
    }

    fn count_rows(&self,
                  expanded: &[ItemRc<Item>],
                  range: &Range<u64>,
                  node_ref: &ItemRc<Item>,
                  offset: u64,
                  end: u64)
        -> Result<(u64, u64), ModelError>
    {
        let rows_before_offset =
            self.count_rows_to(expanded, range, node_ref, offset)?;
        let rows_before_end =
            self.count_rows_to(expanded, range, node_ref, end)?;
        let rows_after_offset = rows_before_end - rows_before_offset;
        Ok((rows_before_offset, rows_after_offset))
    }

    fn count_within(&self,
                    expanded: &[ItemRc<Item>],
                    range: &Range<u64>)
        -> Result<u64, ModelError>
    {
        let mut cap = self.capture.lock().or(Err(LockError))?;
        Ok(self
            .adapt_expanded(expanded)
            .map(|(index, item)| cap.count_within(index, &item, range))
            .collect::<Result<Vec<u64>, CaptureError>>()?
            .iter()
            .sum())
    }

    pub fn insert_region(&mut self,
                         position: u64,
                         node_ref: &ItemRc<Item>,
                         parent_start: u64,
                         parent: &Region<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        use IntervalEnd::*;
        use Source::*;
        let node = node_ref.borrow();

        // Relative position of the new region relative to its parent.
        let relative_position = position - parent_start;

        // Decide whether to add an interleaved region, and if so its end.
        let start = node.interval.start;
        let end = match node.interval.end {
            Incomplete => Some(self.item_count),
            Complete(end) if end > start => Some(end),
            _ => None
        };

        // Construct new parent source, new region and initial model update.
        let (region, mut update) = match (&parent.source, end) {
            // New interleaved region expanded from within an existing one.
            (Interleaved(parent_expanded, _), Some(end)) => {
                let mut expanded = parent_expanded.clone();
                expanded.push(node_ref.clone());
                let range = start..end;
                let rows_changed = range.len() - 1 +
                    self.count_within(parent_expanded, &range)?;
                let rows_added = self.count_rows_to(
                    parent_expanded, &range, node_ref, rows_changed)?;
                (Region {
                    source: Interleaved(expanded, range),
                    offset: 0,
                    length: rows_changed + rows_added,
                },
                ModelUpdate {
                    rows_added,
                    rows_removed: 0,
                    rows_changed,
                })
            },
            // A non-interleaved region.
            (_, None) => (
                Region {
                    source: Children(node_ref.clone()),
                    offset: 0,
                    length: node.child_count,
                },
                ModelUpdate {
                    rows_added: node.child_count,
                    rows_removed: 0,
                    rows_changed: 0
                }
            ),
            // Other combinations are not supported.
            (..) => return
                Err(InternalError(String::from(
                    "Unable to construct region")))
        };

        // Reduce the length of the parent region.
        self.regions.insert(parent_start, Region {
            source: match (&parent.source, &region.source) {
                (Interleaved(expanded, parent_range),
                 Interleaved(_, range)) =>
                    Interleaved(
                        expanded.clone(),
                        parent_range.start..(range.start + 1)),
                (..) => parent.source.clone(),
            },
            offset: parent.offset,
            length: relative_position,
        });

        // Split the parent region if it has rows remaining.
        if relative_position + update.rows_changed < parent.length {
            self.regions
                .entry(position)
                .or_insert_with(|| Region {
                    source: match (&parent.source, &region.source) {
                        (Interleaved(expanded, parent_range),
                         Interleaved(_, range)) =>
                            Interleaved(
                                expanded.clone(),
                                range.end..parent_range.end),
                        (..) => parent.source.clone()
                    },
                    offset: match &region.source {
                        Children(_) => parent.offset + relative_position,
                        Interleaved(..) => 0
                    },
                    length:
                        parent.length - relative_position - update.rows_changed
                });
        }

        // Remove all following regions, to iterate over and replace them.
        let mut following_regions = self.regions
            .split_off(&position)
            .into_iter();

        // Insert the new region.
        self.regions.insert(position, region.clone());

        // For an interleaved source, update all regions that it overlaps.
        if let Interleaved(_, new_range) = &region.source {
            for (start, region) in following_regions.by_ref() {
                match &region.source {
                    Interleaved(expanded, range) => {
                        // Do whatever is necessary to overlap this region.
                        if !self.overlap_region(
                            start, &region,
                            expanded, range,
                            node_ref, new_range,
                            &mut update)?
                        {
                            // No further overlapping regions.
                            break;
                        }
                    }
                    Children(_) => {
                        // Region is self-contained, so just shift it down.
                        self.regions.insert(start + update.rows_added, region);
                    }
                }
            }
        }

        // Shift all remaining regions down by the added rows.
        for (start, region) in following_regions {
            self.regions.insert(start + update.rows_added, region);
        }

        // Update total row count.
        self.row_count += update.rows_added;

        Ok(update)
    }

    pub fn delete_region(&mut self,
                         position: u64,
                         node_ref: &ItemRc<Item>,
                         parent_start: u64,
                         parent: &Region<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        use Source::*;

        // Remove the region starting at this position.
        let region = self.regions.remove(&position).ok_or_else(||
            InternalError(format!(
                "No region to delete at position {}", position)))?;

        // Calculate model update and replace if necessary.
        let mut update = match &region.source {
            // Interleaved region, must be replaced with a modified one.
            Interleaved(expanded, range) => {
                let mut less_expanded = expanded.clone();
                less_expanded.retain(|rc| !Rc::ptr_eq(rc, node_ref));
                let removed_within =
                    self.count_rows_to(
                        expanded, range, node_ref, region.length)?;
                // Replace this region with a new version.
                self.regions.insert(position, Region {
                    source: Interleaved(less_expanded, range.clone()),
                    offset: 0,
                    length: region.length - removed_within,
                });
                ModelUpdate {
                    rows_added: 0,
                    rows_removed: removed_within,
                    rows_changed: region.length - removed_within,
                }
            },
            // Non-interleaved region is just removed.
            Children(_) => ModelUpdate {
                rows_added: 0,
                rows_removed: node_ref.borrow().child_count,
                rows_changed: 0,
            }
        };

        // Remove all following regions, to iterate over and replace them.
        let mut following_regions = self.regions
            .split_off(&(position + 1))
            .into_iter();

        // For an interleaved source, update all regions that it overlapped.
        if let Interleaved(_, removed_range) = &region.source {
            for (start, region) in following_regions.by_ref() {
                match &region.source {
                    Interleaved(expanded, range) => {
                        // Do whatever is necessary to unoverlap this region.
                        if !self.unoverlap_region(
                            start, &region,
                            expanded, range,
                            node_ref, removed_range,
                            &mut update)?
                        {
                            // No further overlapping regions.
                            break;
                        }
                    }
                    Children(_) => {
                        // Region is self-contained, so just shift it up.
                        self.regions.insert(
                            start - update.rows_removed, region);
                    }
                }
            }
        }

        // Shift all following regions up by the removed rows.
        for (start, region) in following_regions {
            self.regions.insert(start - update.rows_removed, region);
        }

        // Merge the preceding and following regions if they have the
        // same source.
        if let Some(next_region) = self.regions.get(&position) {
            match (&parent.source, &next_region.source) {
                (Interleaved(a_exp, a_range), Interleaved(b_exp, b_range))
                    if a_exp.len() == b_exp.len() &&
                        a_exp.iter()
                            .zip(b_exp.iter())
                            .all(|(a, b)| Rc::ptr_eq(a, b)) =>
                {
                    let next_length = next_region.length;
                    let new_range = a_range.start..b_range.end;
                    let parent = self.regions
                        .get_mut(&parent_start)
                        .ok_or_else(||
                            InternalError(String::from(
                                "Invalid parent start")))?;
                    parent.length += next_length;
                    parent.source = Interleaved(a_exp.clone(), new_range);
                    self.regions.remove(&position);
                },
                (Children(a_ref), Children(b_ref))
                    if Rc::ptr_eq(&a_ref, &b_ref) =>
                {
                    let next_length = next_region.length;
                    let parent = self.regions
                        .get_mut(&parent_start)
                        .ok_or_else(||
                            InternalError(String::from(
                                "Invalid parent start")))?;
                    parent.length += next_length;
                    self.regions.remove(&position);
                },
                (..) => {},
            }
        }

        // Update total row count.
        self.row_count -= update.rows_removed;

        Ok(update)
    }

    fn overlap_region(&mut self,
                      start: u64,
                      region: &Region<Item>,
                      expanded: &[ItemRc<Item>],
                      range: &Range<u64>,
                      node_ref: &ItemRc<Item>,
                      new_range: &Range<u64>,
                      update: &mut ModelUpdate)
        -> Result<bool, ModelError>
    {
        use Source::*;

        if range.start >= new_range.end {
            // This region does not overlap, so just move it down.
            self.regions.insert(start + update.rows_added, region.clone());
            return Ok(false)
        }

        // Add this interval to those expanded in this region.
        let mut more_expanded = expanded.to_vec();
        more_expanded.push(node_ref.clone());

        if range.end <= new_range.end {
            // The new interval overlaps this whole region.
            // Count rows added before and after its offset.
            let (added_before_offset, added_after_offset) =
                self.count_rows(expanded, range, node_ref,
                                region.offset,
                                region.offset + region.length)?;
            // Replace with a new region.
            self.regions.insert(start + update.rows_added, Region {
                source: Interleaved(more_expanded, range.clone()),
                offset: region.offset + added_before_offset,
                length: region.length + added_after_offset,
            });
            update.rows_changed += region.length;
            update.rows_added += added_after_offset;
            Ok(true)
        } else {
            // May need to split this region into two, since the new interval
            // only overlaps part of its source.
            let first_range = range.start..new_range.end;
            let second_range = new_range.end..range.end;
            // Work out the row at which this region splits.
            let split_offset = self.count_within(expanded, &first_range)?;
            if region.offset >= split_offset {
                // This region begins after the split, so it doesn't
                // overlap with the new interval.
                self.regions.insert(start + update.rows_added, region.clone());
                Ok(false)
            } else {
                // We need to split this region into two. Work out how many
                // rows are added before and after this region's offset.
                let (added_before_offset, added_after_offset) =
                    self.count_rows(expanded, range, node_ref,
                                    region.offset, split_offset)?;
                // Insert new region for the overlapping part.
                self.regions.insert(start + update.rows_added, Region {
                    source: Interleaved(more_expanded, first_range),
                    offset: region.offset + added_before_offset,
                    length: split_offset + added_after_offset,
                });
                update.rows_changed += region.length;
                update.rows_added += added_after_offset;
                // Insert new region for the non-overlapping part.
                self.regions.insert(start + update.rows_added, Region {
                    source: Interleaved(expanded.to_vec(), second_range),
                    offset: 0,
                    length: region.length - split_offset,
                });
                Ok(true)
            }
        }
    }

    fn unoverlap_region(&mut self,
                        start: u64,
                        region: &Region<Item>,
                        expanded: &[ItemRc<Item>],
                        range: &Range<u64>,
                        node_ref: &ItemRc<Item>,
                        removed_range: &Range<u64>,
                        update: &mut ModelUpdate)
        -> Result<bool, ModelError>
    {
        use Source::*;

        if range.start >= removed_range.end {
            // This region does not overlap, so just move it up.
            self.regions.insert(start - update.rows_removed, region.clone());
            return Ok(false)
        }

        // Remove this interval from those expanded in this region.
        let mut less_expanded = expanded.to_vec();
        less_expanded.retain(|rc| !Rc::ptr_eq(rc, node_ref));

        // The removed interval must overlap this whole region.
        // Count rows added before and after its offset.
        let (_, removed_after_offset) =
            self.count_rows(expanded, range, node_ref,
                            region.offset,
                            region.offset + region.length)?;

        // Replace with a new region if needed.
        if removed_after_offset < region.length {
            self.regions.insert(start - update.rows_removed, Region {
                source: Interleaved(less_expanded, range.clone()),
                offset: region.offset,
                length: region.length - removed_after_offset,
            });
        }

        update.rows_changed += region.length;
        update.rows_removed += removed_after_offset;
        Ok(true)
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

        // Fetch the region this row appears in and clone it.
        let (start, region) = self.regions
            .range(..=position)
            .next_back()
            .ok_or_else(||
                InternalError(format!(
                    "No region before position {}", position)))?;
        let start = *start;
        let region = region.clone();

        // New rows will be added or removed after the current one.
        let position = position + 1;

        // Update the region map.
        let update = if expanded {
            // Insert a new region for this node.
            self.insert_region(position, node_ref, start, &region)?
        } else {
            // Collapse children of this node, from last to first.
            let mut child_rows_removed = 0;
            for (start, region) in self.regions
                .clone()
                .range(position..)
                .rev()
            {
                use Source::*;
                if let Children(child_ref) = &region.source {
                    if child_ref.borrow().parent_is(node_ref)? {
                        // The child is at the row before its region.
                        let child_pos = *start - 1;
                        let update =
                            self.set_expanded(child_ref, child_pos, false)?;
                        child_rows_removed += update.rows_removed;
                    }
                }
            }

            // Delete the region associated with this node.
            let mut update =
                self.delete_region(position, node_ref, start, &region)?;

            // Include results of collapsing children in rows removed.
            update.rows_removed += child_rows_removed;
            update
        };

        // Update the parent node's expanded children.
        let node = node_ref.borrow();
        let parent_ref = node.parent.upgrade().ok_or(ParentDropped)?;
        parent_ref.borrow_mut().set_expanded(node_ref, expanded);

        println!();
        println!("Region map:");
        for (start, region) in self.regions.iter() {
            println!("{}: {:?}", start, region);
        }

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
        Ok(match &region.source {
            // Simple region containing children of a parent item.
            Children(parent_rc) => {
                let parent_item = parent_rc.borrow().item;
                let item = cap.child_item(&parent_item, index)?;
                let parent_rc: NodeRc<Item> = parent_rc.clone();
                self.node(cap, &parent_rc, index, item)?
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
                    NextLevelItem(_, parent_index, child_index, item) => {
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
        min(self.row_count, u32::MAX as u64) as u32
    }

    pub fn item(&self, position: u32) -> Option<Object> {
        // Check row number is valid.
        if position >= self.n_items() {
            return None
        }
        let node_or_err_msg =
            self.fetch(position as u64)
                .map_err(|e| format!("{:?}", e));
        let row_data = RowData::new(node_or_err_msg);
        Some(row_data.upcast::<Object>())
    }
}
