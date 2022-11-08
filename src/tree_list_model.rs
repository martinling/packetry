use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Debug;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::ops::{DerefMut, Range};

use itertools::Itertools;
use thiserror::Error;

use crate::capture::{
    Capture,
    CaptureError,
    ItemSource,
    SearchResult,
    CompletionStatus
};
use crate::id::HasLength;

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
    direct_count: u64,

    /// Total number nodes below this node, recursively.
    total_count: u64,

    /// Expanded children of this item.
    expanded: BTreeMap<u64, ItemNodeRc<Item>>,
}

impl<Item> Children<Item> {
    fn new(child_count: u64) -> Self {
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
    item_index: u64,

    /// Completion status of this item.
    completion: CompletionStatus,

    /// Children of this item.
    children: Children<Item>,
}

impl<Item> Children<Item> {
    /// Whether this child is expanded.
    fn expanded(&self, index: u64) -> bool {
        self.expanded.contains_key(&index)
    }

    /// Get the expanded child with the given index.
    fn get_expanded(&self, index: u64) -> Option<ItemNodeRc<Item>> {
        self.expanded.get(&index).map(Rc::clone)
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
    #[cfg(any(test, feature="record-ui-test"))]
    pub fn item(&self) -> Item {
        self.item
    }

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

#[derive(Clone)]
enum Source<Item> {
    TopLevelItems(),
    ChildrenOf(ItemNodeRc<Item>),
    InterleavedSearch(Vec<ItemNodeRc<Item>>, Range<u64>),
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
            InterleavedSearch(expanded, range) =>
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

fn same_expanded<Item>(a: &[ItemNodeRc<Item>], b: &[ItemNodeRc<Item>]) -> bool {
    a.len() == b.len() &&
        a.iter()
            .zip(b.iter())
            .all(|(a, b)| Rc::ptr_eq(a, b))
}

impl<Item> Region<Item> where Item: Clone {
    fn merge(
        region_a: &Region<Item>,
        region_b: &Region<Item>
    ) -> Option<Region<Item>> {
        match (&region_a.source, &region_b.source) {
            (InterleavedSearch(exp_a, range_a),
             InterleavedSearch(exp_b, range_b))
                if same_expanded(exp_a, exp_b) => Some(
                    Region {
                        source: InterleavedSearch(
                            exp_a.clone(),
                            range_a.start..range_b.end),
                        offset: region_a.offset,
                        length: region_a.length + region_b.length,
                    }
                ),
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

#[derive(Default)]
pub struct ModelUpdate {
    pub rows_added: u64,
    pub rows_removed: u64,
    pub rows_changed: u64,
}

impl core::ops::AddAssign for ModelUpdate {
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

pub struct TreeListModel<Item> {
    capture: Arc<Mutex<Capture>>,
    regions: BTreeMap<u64, Region<Item>>,
    root: Rc<RefCell<RootNode<Item>>>,
    incomplete: Vec<Weak<RefCell<ItemNode<Item>>>>,
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
            incomplete: Vec::new(),
        };
        model.regions.insert(0, Region {
            source: Source::TopLevelItems(),
            offset: 0,
            length: item_count,
        });
        Ok(model)
    }

    pub fn row_count(&self) -> u64 {
        self.root.borrow().children.total_count
    }

    fn item_count(&self) -> u64 {
        self.root.borrow().children().direct_count
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
                let mut cap = self.capture.lock()
                    .or(Err(ModelError::LockError))?;
                Rc::new(RefCell::new(ItemNode {
                    item,
                    parent: Rc::downgrade(parent_rc),
                    item_index: index,
                    completion: cap.item_end(&item, index)?,
                    children: Children::new(cap.item_count(&Some(item))?),
                }))
            }
        })
    }

    fn node_range(&self, node_ref: &ItemNodeRc<Item>) -> Range<u64> {
        use CompletionStatus::*;
        let node = node_ref.borrow();
        let start = node.item_index;
        let end = match node.completion {
            InterleavedComplete(index) => index,
            InterleavedOngoing() => self.item_count(),
            _ => start,
        };
        start..end
    }

    fn adapt_expanded<'exp>(&self, expanded_rcs: &'exp [ItemNodeRc<Item>])
        -> impl Iterator<Item=(u64, Item)> + 'exp
    {
        expanded_rcs.iter().map(|node_rc| {
            let node = node_rc.borrow();
            (node.item_index, node.item)
        })
    }

    fn count_to_item(&self,
                     expanded: &[ItemNodeRc<Item>],
                     range: &Range<u64>,
                     to_index: u64,
                     node_ref: &ItemNodeRc<Item>)
        -> Result<u64, ModelError>
    {
        use SearchResult::*;
        let node = node_ref.borrow();
        let item_index = node.item_index;
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
                           expanded: &[ItemNodeRc<Item>],
                           range: &Range<u64>,
                           node_ref: &ItemNodeRc<Item>,
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
            if end == 0 {
                0
            } else if end == length {
                self.count_within(&[node_ref.clone()], range)?
            } else {
                self.count_to_item(expanded, range, end - 1, node_ref)?
            };
        let rows_after_offset = rows_before_end - rows_before_offset;
        Ok((rows_before_offset, rows_after_offset))
    }

    fn count_within(&self,
                    expanded: &[ItemNodeRc<Item>],
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
              node_ref: &ItemNodeRc<Item>)
        -> Result<ModelUpdate, ModelError>
    {
        // Extract some details of the node being expanded.
        use CompletionStatus::*;
        let node = node_ref.borrow();
        let node_start = node.item_index;
        let interleaved =
            matches!(node.completion,
                     InterleavedComplete(_) | InterleavedOngoing());

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
                        source: ChildrenOf(node_ref.clone()),
                        offset: 0,
                        length: node.children.direct_count,
                    },
                    vec![Region {
                        source: parent.source.clone(),
                        offset: parent.offset + relative_position,
                        length: parent.length - relative_position,
                    }]
                )?
            },
            // Last top level item in a region expanded, but not the last item
            // of the whole model. There must be a following interleaved region,
            // which will be updated later.
            (true, TopLevelItems())
                if (relative_position == parent.length) &&
                    (parent.offset + relative_position != self.item_count()) =>
            {
                let mut update = ModelUpdate::default();
                self.preserve_region(
                    &mut update, parent_start, &parent, false)?;
                update
            },
            // Interleaved region expanded from within a root region.
            (true, TopLevelItems()) => {
                let expanded = vec![node_ref.clone()];
                let range = node_start..(node_start + 1);
                let added = self.count_within(&expanded, &range)?;
                self.split_parent(parent_start, &parent, node_ref,
                    vec![Region {
                        source: TopLevelItems(),
                        offset: parent.offset,
                        length: relative_position,
                    }],
                    Region {
                        source: InterleavedSearch(expanded, range),
                        offset: 0,
                        length: added,
                    },
                    vec![Region {
                        source: TopLevelItems(),
                        offset: parent.offset + relative_position,
                        length: parent.length - relative_position,
                    }]
                )?
            },
            // New interleaved region expanded from within an existing one.
            (true, InterleavedSearch(parent_expanded, parent_range)) => {
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
                        source: InterleavedSearch(parent_expanded.clone(), range_1),
                        offset: 0,
                        length: relative_position - 1,
                    },
                    Region {
                        source: TopLevelItems(),
                        offset: node_start,
                        length: 1,
                    }],
                    Region {
                        source: InterleavedSearch(expanded, range_2),
                        offset: 0,
                        length: changed + added,
                    },
                    if relative_position + changed == parent.length {
                        vec![]
                    } else {
                        vec![Region {
                            source: TopLevelItems(),
                            offset: node_start + 1,
                            length: 1,
                        },
                        Region {
                            source:
                                InterleavedSearch(parent_expanded.clone(), range_3),
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

        drop(node);

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

        // Update total row counts back to the root.
        self.update_totals(node_ref, &update)?;

        // If this item is not yet complete, add it to our incomplete list.
        if matches!(
            node_ref.borrow().completion,
            CompletionStatus::StandaloneOngoing())
        {
            self.incomplete.push(Rc::downgrade(node_ref))
        }

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
                    rows_removed: node_ref.borrow().children.direct_count,
                    rows_changed: 0,
                }
            },
            // For an interleaved source, update all overlapped regions.
            InterleavedSearch(..) => {
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

        // Update total row counts back to the root.
        self.update_totals(node_ref, &update)?;

        Ok(update)
    }

    fn update_totals(&self, node_rc: &ItemNodeRc<Item>, update: &ModelUpdate)
        -> Result<(), ModelError>
    {
        let mut node_opt: Option<AnyNodeRc<Item>> = Some(node_rc.clone());
        while let Some(node_rc) = node_opt {
            let mut node = node_rc.borrow_mut();
            let children = node.children_mut();
            children.total_count += update.rows_added;
            children.total_count -= update.rows_removed;
            node_opt = node.parent()?;
        }
        Ok(())
    }

    fn overlap_region(&mut self,
                      update: &mut ModelUpdate,
                      start: u64,
                      region: &Region<Item>,
                      node_ref: &ItemNodeRc<Item>)
        -> Result<bool, ModelError>
    {
        use Source::*;

        let node_range = self.node_range(node_ref);

        Ok(match &region.source {
            TopLevelItems() if region.offset >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            InterleavedSearch(_, range) if range.start >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            ChildrenOf(_) => {
                // This region is overlapped but self-contained.
                self.preserve_region(update, start, region, true)?
            },
            TopLevelItems() if region.length == 1 => {
                // This region includes only a single root item, and does
                // not need to be translated to an interleaved one.
                self.preserve_region(update, start, region, true)?
            },
            TopLevelItems() if region.offset + region.length == node_range.end &&
                node_range.end == self.item_count() =>
            {
                // This region is fully overlapped and runs to the end
                // of the model, not just the last item.
                let expanded = vec![node_ref.clone()];
                let range = region.offset..self.item_count();
                let changed = range.len() - 1;
                let added = self.count_within(&expanded, &range)?;
                self.replace_region(update, start, region,
                    vec![Region {
                        source: TopLevelItems(),
                        offset: region.offset,
                        length: 1,
                    },
                    Region {
                        source: InterleavedSearch(expanded, range),
                        offset: 0,
                        length: changed + added,
                    }]
                )?;
                true
            }
            TopLevelItems() if region.offset + region.length <= node_range.end => {
                // This region is fully overlapped by the new node.
                // Replace with a new interleaved region.
                let expanded = vec![node_ref.clone()];
                let range = region.offset..(region.offset + region.length - 1);
                let added = self.count_within(&expanded, &range)?;
                self.replace_region(update, start, region,
                    vec![Region {
                        source: TopLevelItems(),
                        offset: region.offset,
                        length: 1,
                    },
                    Region {
                        source: InterleavedSearch(expanded, range),
                        offset: 0,
                        length: region.length - 2 + added,
                    },
                    Region {
                        source: TopLevelItems(),
                        offset: region.offset + region.length - 1,
                        length: 1
                    }]
                )?;
                true
            },
            TopLevelItems() => {
                // This region is partially overlapped by the new node.
                // Split it into overlapped and unoverlapped parts.
                let expanded = vec![node_ref.clone()];
                let range = region.offset..node_range.end;
                let changed = range.len() - 1;
                let added = self.count_within(&expanded, &range)?;
                self.partial_overlap(update, start, region,
                    vec![Region {
                        source: TopLevelItems(),
                        offset: region.offset,
                        length: 1,
                    },
                    Region {
                        source: InterleavedSearch(expanded, range),
                        offset: 0,
                        length: changed + added
                    }],
                    vec![Region {
                        source: TopLevelItems(),
                        offset: region.offset + changed + 1,
                        length: region.length - changed - 1,
                    }]
                )?;
                // No longer overlapping.
                false
            },
            InterleavedSearch(expanded, range) if range.end <= node_range.end => {
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
                        source: InterleavedSearch(more_expanded, range.clone()),
                        offset: region.offset + added_before_offset,
                        length: region.length + added_after_offset,
                    }]
                )?;
                true
            },
            InterleavedSearch(expanded, range) => {
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
                            source: InterleavedSearch(more_expanded, first_range),
                            offset: region.offset + added_before_offset,
                            length: split_offset + added_after_offset,
                        }],
                        vec![Region {
                            source: TopLevelItems(),
                            offset: node_range.end,
                            length: 1,
                        },
                        Region {
                            source: InterleavedSearch(expanded.clone(), second_range),
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
                        node_ref: &ItemNodeRc<Item>)
        -> Result<bool, ModelError>
    {
        use Source::*;

        let node_range = self.node_range(node_ref);

        Ok(match &region.source {
            TopLevelItems() if region.offset >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            InterleavedSearch(_, range) if range.start >= node_range.end => {
                // This region is not overlapped.
                self.preserve_region(update, start, region, false)?
            },
            ChildrenOf(_) | TopLevelItems() => {
                // This region is overlapped but self-contained.
                self.preserve_region(update, start, region, true)?
            },
            InterleavedSearch(expanded, range) => {
                // This region is overlapped. Replace with a new one.
                let mut less_expanded = expanded.to_vec();
                less_expanded.retain(|rc| !Rc::ptr_eq(rc, node_ref));
                let new_region = if less_expanded.is_empty() {
                    // This node was the last expanded one in this region.
                    Region {
                        source: TopLevelItems(),
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
                        source: InterleavedSearch(less_expanded, range.clone()),
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
                        "At position {}, overwriting region", position)))
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
        use std::cmp::Ordering::*;

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
                    node_ref: &ItemNodeRc<Item>,
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

        let interleaved = matches!(&new_region.source, InterleavedSearch(..));
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

    fn merge_pairs(&mut self) {
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

    fn merge_regions(&mut self) {
        #[cfg(feature="debug-region-map")]
        {
            println!();
            println!("Before merge:");
            for (start, region) in self.regions.iter() {
                println!("{}: {:?}", start, region);
            }
        }

        // Merge adjacent regions with the same source.
        self.merge_pairs();

        // Find starts and lengths of superfluous root regions.
        let superfluous_regions: Vec<(u64, u64)> = self.regions
            .iter()
            .tuple_windows()
            .filter_map(|((_, a), (b_start, b), (_, c))|
                 match (&a.source, &b.source, &c.source) {
                    (InterleavedSearch(exp_a, _),
                     TopLevelItems(),
                     InterleavedSearch(exp_c, _))
                        if same_expanded(exp_a, exp_c) => {
                            #[cfg(feature="debug-region-map")]
                            {
                                println!();
                                println!("Dropping: {:?}", b);
                            }
                            Some((*b_start, b.length))
                        },
                    _ => None
                 })
            .collect();

        // Remove superfluous regions.
        for (start, length) in superfluous_regions {
            self.regions.remove(&start);
            let (_, next_region) = self.regions
                .range_mut(start..)
                .next()
                .unwrap();
            next_region.length += length;
        }

        // Once again merge adjacent regions with the same source.
        self.merge_pairs();
    }

    pub fn update(&mut self) -> Result<Vec<(u64, ModelUpdate)>, ModelError> {

        // List of positions and model updates to be returned.
        let mut updates: Vec<(u64, ModelUpdate)> = Vec::new();

        // Iterate over incomplete items, extending their regions as needed.
        // Weak references that are no longer valid are dropped, since the
        // referenced nodes must no longer be expanded.
        for weak_rc in self.incomplete.split_off(0) {
            if let Some(node_rc) = weak_rc.upgrade() {
                use {CompletionStatus::*, Source::*};
                let mut node = node_rc.borrow_mut();
                let mut cap = self.capture.lock()
                    .or(Err(ModelError::LockError))?;

                // Count children before update, after update, and added.
                let old_child_count = node.children.direct_count;
                let new_child_count = cap.item_count(&Some(node.item))?;
                let update = ModelUpdate {
                    rows_added: new_child_count - old_child_count,
                    rows_removed: 0,
                    rows_changed: 0,
                };

                // Update child count and completion status.
                node.children.direct_count = new_child_count;
                node.completion = cap.item_end(&node.item, node.item_index)?;

                drop(cap);

                if matches!(node.completion, StandaloneOngoing()) {
                    // This node is still incomplete, keep it in the list.
                    self.incomplete.push(weak_rc);
                }

                // Find the start of this item's rows.
                let start = self.regions
                    .iter()
                    .find_map(|(start, region)| {
                        if let ChildrenOf(rc) = &region.source {
                            if Rc::ptr_eq(rc, &node_rc) {
                                return Some(start)
                            }
                        };
                        None
                    })
                    .unwrap();

                // Find the end of this item's rows.
                let end = start + node.children.total_count;

                // Check if the former last child of this item is expanded.
                let last_expanded =
                    old_child_count > 0 &&
                    node.children.expanded(old_child_count - 1);

                drop(node);

                // If so, add a new region after all this node's rows.
                if last_expanded {
                    self.insert_region(
                        end,
                        Region {
                            source: ChildrenOf(node_rc.clone()),
                            offset: old_child_count,
                            length: update.rows_added,
                        })?;
                // Otherwise, extend the last region in this node's rows.
                } else {
                    let (_start, region) = self.regions
                        .range_mut(..end)
                        .next_back()
                        .unwrap();
                    region.length += update.rows_added;
                }

                // Update total row counts back to the root.
                self.update_totals(&node_rc, &update)?;

                // Add this update to the list of updates to be applied.
                updates.push((end, update));
            }
        }

        // Handle new top-level items that were added.

        let mut cap = self.capture.lock().or(Err(ModelError::LockError))?;

        let mut root = self.root.borrow_mut();
        let new_item_count = cap.item_count(&None)?;
        let old_item_count = root.children.direct_count;

        drop(cap);

        if new_item_count == old_item_count {
            return Ok(updates);
        }

        let position = root.children.total_count;
        let update = ModelUpdate {
            rows_added: new_item_count - old_item_count,
            rows_removed: 0,
            rows_changed: 0,
        };

        root.children.direct_count = new_item_count;
        root.children.total_count += update.rows_added;

        drop(root);

        // Update the region map to add new top level items.
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
            Some((_start, region)) => match &mut region.source {
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
                },
                InterleavedSearch(expanded, _range) => {
                    // Last region is an interleaved search. Some of the
                    // expanded items in it may have now ended. Update it,
                    // perhaps breaking it into multiple regions.

                    // The items expanded in this region that have completed.
                    let mut completed: BTreeMap<u64, ItemNodeRc<Item>> =
                        BTreeMap::new();

                    let mut cap = self.capture.lock()
                        .or(Err(ModelError::LockError))?;

                    for node_rc in expanded {
                        let mut node = node_rc.borrow_mut();
                        // Update completion status for this node.
                        node.completion =
                            cap.item_end(&node.item, node.item_index)?;

                        // If this item completed, make a note of it.
                        use CompletionStatus::*;
                        if let InterleavedComplete(end) = node.completion {
                            completed.insert(end, node_rc.clone());
                        }
                    }

                    drop(cap);

                    // Now split the region for each item that completed.
                    for (_end, _node_rc) in completed {
                        unimplemented!();
                    }
                }
            }
        };

        // Add the update for the new top level items to the list.
        updates.push((position, update));

        Ok(updates)
    }

    pub fn fetch(&self, position: u64)
        -> Result<ItemNodeRc<Item>, ModelError>
    {
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
                drop(cap);
                self.node(&parent_rc, index, item)?
            },
            // Region containing children of a parent item.
            ChildrenOf(parent_rc) => {
                let parent_item = parent_rc.borrow().item;
                let item = cap.child_item(&parent_item, index)?;
                let parent_rc: AnyNodeRc<Item> = parent_rc.clone();
                drop(cap);
                self.node(&parent_rc, index, item)?
            },
            // Region requiring interleaved search.
            InterleavedSearch(expanded_rcs, range) => {
                // Run the interleaved search.
                let mut expanded = self.adapt_expanded(expanded_rcs);
                let search_result =
                    cap.find_child(&mut expanded, range, index)?;
                drop(cap);
                drop(expanded);
                // Return a node corresponding to the search result.
                use SearchResult::*;
                match search_result {
                    // Search found a top level item.
                    TopLevelItem(index, item) => {
                        let root_rc: AnyNodeRc<Item> = self.root.clone();
                        self.node(&root_rc, index, item)?
                    },
                    // Search found a child of an expanded top level item.
                    NextLevelItem(_, parent_index, child_index, item) => {
                        // There must already be a node for its parent.
                        let parent_rc: AnyNodeRc<Item> = self.root
                            .borrow()
                            .children()
                            .get_expanded(parent_index)
                            .ok_or(ParentDropped)?;
                        self.node(&parent_rc, child_index, item)?
                    }
                }
            }
        })
    }
}
