use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::cmp::{min, Ordering};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::TryFromIntError;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex, MutexGuard};
use std::ops::{AddAssign, DerefMut, Range};

use gtk::prelude::{IsA, Cast};
use gtk::glib::Object;
use itertools::Itertools;
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
type NodeRc<Item> = Rc<RefCell<dyn Node<Item>>>;

trait Node<Item> {
    /// Access the expanded children of this node.
    fn expanded_children(&self) -> &ExpandedChildren<Item>;

    /// Mutably access the expanded children of this node.
    fn expanded_children_mut(&mut self) -> &mut ExpandedChildren<Item>;
}

struct ExpandedChildren<Item> {
    map: BTreeMap<u64, ItemRc<Item>>
}

struct RootNode<Item> {
    /// Expanded top level items.
    expanded: ExpandedChildren<Item>,
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

    /// Expanded children of this item.
    expanded: ExpandedChildren<Item>,
}

impl<Item> ExpandedChildren<Item> {
    /// Whether the owning node has this child node expanded.
    fn contains(&self, node: &ItemNode<Item>) -> bool {
        self.map.contains_key(&node.interval.start)
    }

    /// Get the expanded child node with this index.
    fn get(&self, index: u64) -> Option<ItemRc<Item>> {
        self.map.get(&index).map(Rc::clone)
    }

    /// Set whether this child of the owning node is expanded.
    fn set(&mut self, child_rc: &ItemRc<Item>, expanded: bool) {
        let child = child_rc.borrow();
        let start = child.interval.start;
        if expanded {
            self.map.insert(start, child_rc.clone());
        } else {
            self.map.remove(&start);
        }
    }
}

impl<Item> Node<Item> for RootNode<Item> {
    fn expanded_children(&self) -> &ExpandedChildren<Item> {
        &self.expanded
    }

    fn expanded_children_mut(&mut self) -> &mut ExpandedChildren<Item> {
        &mut self.expanded
    }
}

impl<Item> Node<Item> for ItemNode<Item> {
    fn expanded_children(&self) -> &ExpandedChildren<Item> {
        &self.expanded
    }

    fn expanded_children_mut(&mut self) -> &mut ExpandedChildren<Item> {
        &mut self.expanded
    }
}

impl<Item> ItemNode<Item>
where Item: 'static + Copy
{
    #[cfg(any(test, feature="record-ui-test"))]
    pub fn item(&self) -> Item {
        self.item
    }

    fn parent_is(&self, node_ref: &ItemRc<Item>)
        -> Result<bool, ModelError>
    {
        let parent_ref = self.parent.upgrade().ok_or(ParentDropped)?;
        let node_ref: NodeRc<Item> = node_ref.clone();
        Ok(Rc::ptr_eq(&parent_ref, &node_ref))
    }

    pub fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_rc) => parent_rc
                .borrow()
                .expanded_children()
                .contains(self),
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
enum Source<Item> {
    Root(),
    Children(ItemRc<Item>),
    Interleaved(Vec<ItemRc<Item>>, Range<u64>),
}

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
            Root() =>
                write!(f, "Top level items"),
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

impl<Item> Region<Item>
where Item: Clone + Debug
{
    fn merge(
        region_a: &Region<Item>,
        region_b: &Region<Item>
    ) -> Option<Region<Item>> {
        use Source::*;
        match (&region_a.source, &region_b.source) {
            (Interleaved(exp_a, range_a),
             Interleaved(exp_b, range_b))
                if exp_a.len() == exp_b.len() &&
                    exp_a.iter()
                        .zip(exp_b.iter())
                        .all(|(a, b)| Rc::ptr_eq(a, b)) => Some(
                    Region {
                        source: Interleaved(
                            exp_a.clone(),
                            range_a.start..range_b.end),
                        offset: region_a.offset,
                        length: region_a.length + region_b.length,
                    }
                ),
            (Children(a_ref), Children(b_ref))
                if Rc::ptr_eq(a_ref, b_ref) => Some(
                    Region {
                        source: region_a.source.clone(),
                        offset: region_a.offset,
                        length: region_a.length + region_b.length,
                    }
                ),
            (Root(), Root()) => Some(
                Region {
                    source: Root(),
                    offset: region_a.offset,
                    length: region_a.length + region_b.length,
                }
            ),
            (..) => None,
        }
    }
}

#[derive(Default)]
pub struct ModelUpdate {
    pub rows_added: u64,
    pub rows_removed: u64,
    pub rows_changed: u64,
}

impl AddAssign for ModelUpdate {
    fn add_assign(&mut self, other: ModelUpdate) {
        self.rows_added += other.rows_added;
        self.rows_removed += other.rows_removed;
        self.rows_changed += other.rows_changed;
    }
}

impl Debug for ModelUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>)
        -> Result<(), std::fmt::Error>
    {
        write!(f, "{} added, {} removed, {} changed",
            self.rows_added, self.rows_removed, self.rows_changed)
    }
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
        let item_count = cap.item_count()?;
        let mut model = TreeListModel {
            _marker: PhantomData,
            capture: capture.clone(),
            item_count,
            row_count: item_count,
            regions: BTreeMap::new(),
            root: Rc::new(RefCell::new(RootNode {
                expanded: ExpandedChildren {
                    map: BTreeMap::new(),
                }
            })),
        };
        model.regions.insert(0, Region {
            source: Root(),
            offset: 0,
            length: item_count,
        });
        Ok(model)
    }

    fn node(&self,
                mut cap: MutexGuard<'_, Capture>,
                parent_rc: &NodeRc<Item>,
                index: u64,
                item: Item)
        -> Result<ItemRc<Item>, ModelError>
    {
        let parent = parent_rc.borrow();
        Ok(match parent.expanded_children().get(index) {
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
                    expanded: ExpandedChildren {
                        map: BTreeMap::new()
                    }
                }))
            }
        })
    }

    fn range(&self, node_ref: &ItemRc<Item>) -> Range<u64> {
        use IntervalEnd::*;
        let node = node_ref.borrow();
        let start = node.interval.start;
        let end = match node.interval.end {
            Incomplete => self.item_count,
            Complete(end) => end,
        };
        start..end
    }

    fn adapt_expanded<'exp>(&self, expanded_rcs: &'exp [ItemRc<Item>])
        -> impl Iterator<Item=(u64, Item)> + 'exp
    {
        expanded_rcs.iter().map(|node_rc| {
            let node = node_rc.borrow();
            (node.interval.start, node.item)
        })
    }

    fn count_to_item(&self,
                     expanded: &[ItemRc<Item>],
                     range: &Range<u64>,
                     to_index: u64,
                     node_ref: &ItemRc<Item>)
        -> Result<u64, ModelError>
    {
        use SearchResult::*;
        let node = node_ref.borrow();
        let item_index = node.interval.start;
        let item = &node.item;
        let mut expanded = self.adapt_expanded(expanded);
        let mut cap = self.capture.lock().or(Err(LockError))?;
        let search_result = cap.find_child(&mut expanded, range, to_index);
        Ok(match search_result {
            Ok(TopLevelItem(found_index, _)) => {
                let range_to_item = range.start..found_index;
                cap.count_within(item_index, item, &range_to_item)?
            },
            Ok(NextLevelItem(span_index, .., child)) => {
                let range_to_span = range.start..span_index;
                cap.count_within(item_index, item, &range_to_span)? +
                    cap.count_before(item_index, item, span_index, &child)?
            },
            Err(_) => cap.count_within(item_index, item, range)?
        })
    }

    fn count_around_offset(&self,
                           expanded: &[ItemRc<Item>],
                           range: &Range<u64>,
                           node_ref: &ItemRc<Item>,
                           offset: u64,
                           end: u64)
        -> Result<(u64, u64), ModelError>
    {
        let length = range.len() - 1 + self.count_within(expanded, range)?;
        let rows_before_offset =
            if offset == 0 {
                0
            } else {
                self.count_to_item(expanded, range, offset - 1, node_ref)?
            };
        let rows_before_end =
            if end == length {
                self.count_within(&[node_ref.clone()], range)?
            } else {
                self.count_to_item(expanded, range, end - 1, node_ref)?
            };
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

    fn expand(&mut self,
                  position: u64,
                  node_ref: &ItemRc<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        use IntervalEnd::*;
        use Source::*;

        // Extract some details of the node being expanded.
        let node = node_ref.borrow();
        let node_start = node.interval.start;
        let interleaved = !matches!(node.interval.end,
            Complete(end) if end == node_start);

        // Find the start of the parent region.
        let (&parent_start, _) = self.regions
            .range(..position)
            .next_back()
            .ok_or_else(||
                InternalError(format!(
                    "No region before position {}", position)))?;

        // Find position of the new node relative to its parent.
        let relative_position = position - parent_start;

        // Remove the parent.
        let parent = self.regions
            .remove(&parent_start)
            .ok_or_else(||
                InternalError(format!(
                    "Parent not found at position {}", parent_start)))?;

        // Remove all following regions, to iterate over later.
        let mut following_regions = self.regions
            .split_off(&parent_start)
            .into_iter();

        // Split the parent region and construct a new region between.
        //
        // Where the new region is an interleaved one, its overlap with the
        // remainder of the parent is handled in the split_parent call.
        let mut update = match (interleaved, &parent.source) {
            // Self-contained region expanded.
            (false, _) => {
                self.split_parent(parent_start, &parent, node_ref,
                    vec![Region {
                        source: parent.source.clone(),
                        offset: parent.offset,
                        length: relative_position,
                    }],
                    Region {
                        source: Children(node_ref.clone()),
                        offset: 0,
                        length: node.child_count,
                    },
                    vec![Region {
                        source: parent.source.clone(),
                        offset: parent.offset + relative_position,
                        length: parent.length - relative_position,
                    }]
                )?
            },
            // Last root item in a region expanded, but not the last item of
            // the whole model. There must be a following interleaved region,
            // which will be updated later.
            (true, Root())
                if (relative_position == parent.length) &&
                    (parent.offset + relative_position != self.item_count) =>
            {
                let mut update = ModelUpdate::default();
                self.preserve_region(
                    &mut update, parent_start, &parent, false)?;
                update
            },
            // Interleaved region expanded from within a root region.
            (true, Root()) => {
                let expanded = vec![node_ref.clone()];
                let range = node_start..(node_start + 1);
                let added = self.count_within(&expanded, &range)?;
                self.split_parent(parent_start, &parent, node_ref,
                    vec![Region {
                        source: Root(),
                        offset: parent.offset,
                        length: relative_position,
                    }],
                    Region {
                        source: Interleaved(expanded, range),
                        offset: 0,
                        length: added,
                    },
                    vec![Region {
                        source: Root(),
                        offset: parent.offset + relative_position,
                        length: parent.length - relative_position,
                    }]
                )?
            },
            // New interleaved region expanded from within an existing one.
            (true, Interleaved(parent_expanded, parent_range)) => {
                assert!(parent.offset == 0);
                let mut expanded = parent_expanded.clone();
                expanded.push(node_ref.clone());
                let range_1 = parent_range.start..node_start;
                let range_2 = node_start..(node_start + 1);
                let range_3 = range_2.end..parent_range.end;
                let changed = self.count_within(parent_expanded, &range_2)?;
                let added = self.count_within(&[node_ref.clone()], &range_2)?;
                self.split_parent(parent_start, &parent, node_ref,
                    vec![Region {
                        source: Interleaved(parent_expanded.clone(), range_1),
                        offset: 0,
                        length: relative_position - 1,
                    },
                    Region {
                        source: Root(),
                        offset: node_start,
                        length: 1,
                    }],
                    Region {
                        source: Interleaved(expanded, range_2),
                        offset: 0,
                        length: changed + added,
                    },
                    if relative_position + changed == parent.length {
                        vec![]
                    } else {
                        vec![Region {
                            source: Root(),
                            offset: node_start + 1,
                            length: 1,
                        },
                        Region {
                            source:
                                Interleaved(parent_expanded.clone(), range_3),
                            offset: 0,
                            length:
                                parent.length - relative_position - changed - 1,
                        }]
                    }
                )?
            },
            // Other combinations are not supported.
            (..) => return
                Err(InternalError(format!(
                    "Unable to expand from {:?}", parent)))
        };

        // For an interleaved source, update all regions that it overlaps.
        if interleaved {
            for (start, region) in following_regions.by_ref() {
                // Do whatever is necessary to overlap this region.
                if !self.overlap_region(&mut update, start, &region, node_ref)?
                {
                    // No further overlapping regions.
                    break;
                }
            }
        }

        // Shift all remaining regions down by the added rows.
        for (start, region) in following_regions {
            self.insert_region(start + update.rows_added, region)?;
        }

        // Merge adjacent regions with the same source.
        self.merge_regions();

        // Update total row count.
        self.row_count += update.rows_added;

        Ok(update)
    }

    fn collapse(&mut self,
                    position: u64,
                    node_ref: &ItemRc<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        use Source::*;

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
            Root() => return Err(InternalError(String::from(
                "Unable to collapse root region"))),
            // Non-interleaved region is just removed.
            Children(_) => {
                let (_, _region) = following_regions.next().unwrap();
                #[cfg(feature="debug-region-map")]
                {
                    println!();
                    println!("Removing: {:?}", _region);
                }
                ModelUpdate {
                    rows_added: 0,
                    rows_removed: node_ref.borrow().child_count,
                    rows_changed: 0,
                }
            },
            // For an interleaved source, update all overlapped regions.
            Interleaved(..) => {
                let mut update = ModelUpdate::default();
                for (start, region) in following_regions.by_ref() {
                    // Do whatever is necessary to unoverlap this region.
                    if !self.unoverlap_region(
                        &mut update, start, &region, node_ref)?
                    {
                        // No further overlapping regions.
                        break;
                    }
                }
                update
            }
        };

        // Shift all following regions up by the removed rows.
        for (start, region) in following_regions {
            self.insert_region(start - update.rows_removed, region)?;
        }

        // Merge adjacent regions with the same source.
        self.merge_regions();

        // Update total row count.
        self.row_count -= update.rows_removed;

        Ok(update)
    }

    fn overlap_region(&mut self,
                      update: &mut ModelUpdate,
                      start: u64,
                      region: &Region<Item>,
                      node_ref: &ItemRc<Item>)
        -> Result<bool, ModelError>
    {
        use Source::*;

        let node_range = self.range(node_ref);

        Ok(match &region.source {
            Root() if region.offset >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            Interleaved(_, range) if range.start >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            Children(_) => {
                // This region is overlapped but self-contained.
                self.preserve_region(update, start, region, true)?
            },
            Root() if region.length == 1 => {
                // This region includes only a single root item, and does
                // not need to be translated to an interleaved one.
                self.preserve_region(update, start, region, true)?
            },
            Root() if region.offset + region.length == node_range.end &&
                node_range.end == self.item_count =>
            {
                // This region is fully overlapped and runs to the end
                // of the model, not just the last item.
                let expanded = vec![node_ref.clone()];
                let range = region.offset..self.item_count;
                let changed = range.len() - 1;
                let added = self.count_within(&expanded, &range)?;
                self.replace_region(update, start, region,
                    vec![Region {
                        source: Root(),
                        offset: region.offset,
                        length: 1,
                    },
                    Region {
                        source: Interleaved(expanded, range),
                        offset: 0,
                        length: changed + added,
                    }]
                )?;
                true
            }
            Root() if region.offset + region.length <= node_range.end => {
                // This region is fully overlapped by the new node.
                // Replace with a new interleaved region.
                let expanded = vec![node_ref.clone()];
                let range = region.offset..(region.offset + region.length - 1);
                let added = self.count_within(&expanded, &range)?;
                self.replace_region(update, start, region,
                    vec![Region {
                        source: Root(),
                        offset: region.offset,
                        length: 1,
                    },
                    Region {
                        source: Interleaved(expanded, range),
                        offset: 0,
                        length: region.length - 2 + added,
                    },
                    Region {
                        source: Root(),
                        offset: region.offset + region.length - 1,
                        length: 1
                    }]
                )?;
                true
            },
            Root() => {
                // This region is partially overlapped by the new node.
                // Split it into overlapped and unoverlapped parts.
                let expanded = vec![node_ref.clone()];
                let range = region.offset..node_range.end;
                let changed = range.len() - 1;
                let added = self.count_within(&expanded, &range)?;
                self.partial_overlap(update, start, region,
                    vec![Region {
                        source: Root(),
                        offset: region.offset,
                        length: 1,
                    },
                    Region {
                        source: Interleaved(expanded, range),
                        offset: 0,
                        length: changed + added
                    }],
                    vec![Region {
                        source: Root(),
                        offset: region.offset + changed + 1,
                        length: region.length - changed - 1,
                    }]
                )?;
                // No longer overlapping.
                false
            },
            Interleaved(expanded, range) if range.end <= node_range.end => {
                // This region is fully overlapped by the new node.
                // Replace with a new interleaved region.
                let (added_before_offset, added_after_offset) =
                    self.count_around_offset(
                        expanded, range, node_ref,
                        region.offset,
                        region.offset + region.length)?;
                let mut more_expanded = expanded.clone();
                more_expanded.push(node_ref.clone());
                self.replace_region(update, start, region,
                    vec![Region {
                        source: Interleaved(more_expanded, range.clone()),
                        offset: region.offset + added_before_offset,
                        length: region.length + added_after_offset,
                    }]
                )?;
                true
            },
            Interleaved(expanded, range) => {
                // This region may be partially overlapped by the new node,
                // depending on its offset within its source rows.
                let first_range = range.start..node_range.end;
                let second_range = node_range.end..range.end;
                // Work out the offset at which this source would be split.
                let split_offset = first_range.len() - 1 +
                    self.count_within(expanded, &first_range)?;
                if region.offset > split_offset {
                    // This region begins after the split, so isn't overlapped.
                    self.preserve_region(update, start, region, false)?
                } else {
                    // Split the region into overlapped and unoverlapped parts.
                    let (added_before_offset, added_after_offset) =
                        self.count_around_offset(
                            expanded, range, node_ref,
                            region.offset, split_offset)?;
                    let mut more_expanded = expanded.clone();
                    more_expanded.push(node_ref.clone());
                    self.partial_overlap(update, start, region,
                        vec![Region {
                            source: Interleaved(more_expanded, first_range),
                            offset: region.offset + added_before_offset,
                            length: split_offset + added_after_offset,
                        }],
                        vec![Region {
                            source: Root(),
                            offset: node_range.end,
                            length: 1,
                        },
                        Region {
                            source: Interleaved(expanded.clone(), second_range),
                            offset: 0,
                            length: region.length - split_offset - 1,
                        }]
                    )?;
                    // No longer overlapping.
                    false
                }
            }
        })
    }

    fn unoverlap_region(&mut self,
                        update: &mut ModelUpdate,
                        start: u64,
                        region: &Region<Item>,
                        node_ref: &ItemRc<Item>)
        -> Result<bool, ModelError>
    {
        use Source::*;

        let node_range = self.range(node_ref);

        Ok(match &region.source {
            Root() if region.offset >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            Interleaved(_, range) if range.start >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            Children(_) | Root() => {
                // This region is overlapped but self-contained.
                self.preserve_region(update, start, region, true)?
            },
            Interleaved(expanded, range) => {
                // This region is overlapped. Replace with a new one.
                let mut less_expanded = expanded.to_vec();
                less_expanded.retain(|rc| !Rc::ptr_eq(rc, node_ref));
                let new_region = if less_expanded.is_empty() {
                    // This node was the last expanded one in this region.
                    Region {
                        source: Root(),
                        offset: range.start + 1,
                        length: range.len() - 1,
                    }
                } else {
                    // There are other nodes expanded in this region.
                    let (removed_before_offset, removed_after_offset) =
                        self.count_around_offset(
                            expanded, range, node_ref,
                            region.offset,
                            region.offset + region.length)?;
                    Region {
                        source: Interleaved(less_expanded, range.clone()),
                        offset: region.offset - removed_before_offset,
                        length: region.length - removed_after_offset,
                    }
                };
                self.replace_region(update, start, region, vec![new_region])?;
                true
            }
        })
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
                        "At position {}, overwriting {:?} with {:?}",
                        position, entry.get(), region)))
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(region);
                Ok(())
            }
        }
    }

    fn preserve_region(&mut self,
                       update: &mut ModelUpdate,
                       start: u64,
                       region: &Region<Item>,
                       include_as_changed: bool)
        -> Result<bool, ModelError>
    {
        let new_position = start
            + update.rows_added
            - update.rows_removed;

        self.insert_region(new_position, region.clone())?;

        if include_as_changed {
            update.rows_changed += region.length;
        }

        Ok(include_as_changed)
    }

    fn replace_region(&mut self,
                      update: &mut ModelUpdate,
                      start: u64,
                      region: &Region<Item>,
                      new_regions: Vec<Region<Item>>)
        -> Result<(), ModelError>
    {
        use Ordering::*;

        let new_length: u64 = new_regions
            .iter()
            .map(|region| region.length)
            .sum();

        let effect = match new_length.cmp(&region.length) {
            Greater => ModelUpdate {
                rows_added: new_length - region.length,
                rows_removed: 0,
                rows_changed: region.length
            },
            Less => ModelUpdate {
                rows_added: 0,
                rows_removed: region.length - new_length,
                rows_changed: new_length
            },
            Equal => ModelUpdate {
                rows_added: 0,
                rows_removed: 0,
                rows_changed: region.length
            },
        };

        #[cfg(feature="debug-region-map")]
        {
            println!();
            println!("Replacing: {:?}", region);
            for new_region in new_regions.iter() {
                println!("     with: {:?}", new_region);
            }
            println!("           {:?}", effect);
        }

        let mut position = start
            + update.rows_added
            - update.rows_removed;

        for region in new_regions {
            let length = region.length;
            self.insert_region(position, region)?;
            position += length;
        }

        *update += effect;

        Ok(())
    }

    fn partial_overlap(&mut self,
                       update: &mut ModelUpdate,
                       start: u64,
                       region: &Region<Item>,
                       changed_regions: Vec<Region<Item>>,
                       unchanged_regions: Vec<Region<Item>>)
        -> Result<(), ModelError>
    {
        let changed_length: u64 = changed_regions
            .iter()
            .map(|region| region.length)
            .sum();

        let unchanged_length: u64 = unchanged_regions
            .iter()
            .map(|region| region.length)
            .sum();

        let total_length = changed_length + unchanged_length;

        let effect = ModelUpdate {
            rows_added: total_length - region.length,
            rows_removed: 0,
            rows_changed: region.length - unchanged_length,
        };

        #[cfg(feature="debug-region-map")]
        {
            println!();
            println!("Splitting: {:?}", region);
            for changed_region in changed_regions.iter() {
                println!("         : {:?}", changed_region);
            }
            for unchanged_region in unchanged_regions.iter() {
                println!("         : {:?}", unchanged_region);
            }
            println!("           {:?}", effect);
        }

        let mut position = start + update.rows_added - update.rows_removed;
        for region in changed_regions
            .into_iter()
            .chain(unchanged_regions)
        {
            let length = region.length;
            self.insert_region(position, region)?;
            position += length;
        }

        *update += effect;

        Ok(())
    }

    fn split_parent(&mut self,
                    parent_start: u64,
                    parent: &Region<Item>,
                    node_ref: &ItemRc<Item>,
                    parts_before: Vec<Region<Item>>,
                    new_region: Region<Item>,
                    parts_after: Vec<Region<Item>>)
        -> Result<ModelUpdate, ModelError>
    {
        use Source::*;

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

        let mut update = ModelUpdate {
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

        let interleaved = matches!(&new_region.source, Interleaved(..));
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
            if interleaved {
                self.overlap_region(
                    &mut update,
                    position - rows_added,
                    &region,
                    node_ref)?;
            } else {
                self.insert_region(position, region)?;
            }
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

    pub fn set_expanded(&mut self,
                        node_ref: &ItemRc<Item>,
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
            .expanded_children_mut()
            .set(node_ref, expanded);

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
            // Simple region containing top level items.
            Root() => {
                let item = cap.item(index)?;
                let parent_rc: NodeRc<Item> = self.root.clone();
                self.node(cap, &parent_rc, index, item)?
            },
            // Simple region containing children of a parent item.
            Children(parent_rc) => {
                let parent_item = parent_rc.borrow().item;
                let item = cap.child_item(&parent_item, index)?;
                let parent_rc: NodeRc<Item> = parent_rc.clone();
                self.node(cap, &parent_rc, index, item)?
            },
            // Region requiring interleaved search.
            Interleaved(expanded_rcs, range) => {
                // Run the interleaved search.
                let mut expanded = self.adapt_expanded(expanded_rcs);
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
                            .expanded_children()
                            .get(parent_index)
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
