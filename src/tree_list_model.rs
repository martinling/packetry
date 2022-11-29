use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;

use thiserror::Error;

use crate::capture::{Capture, CaptureError, ItemSource, CompletionStatus};

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
}

pub type ItemNodeRc<Item> = Rc<RefCell<ItemNode<Item>>>;
pub type ItemNodeWeak<Item> = Weak<RefCell<ItemNode<Item>>>;
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

    /// Whether the children of this node are displayed.
    fn expanded(&self) -> bool;

    /// Update total row counts for this node and its parents.
    fn update_total(&mut self, update: &ModelUpdate) -> Result<(), ModelError>;

    /// Mark this node as completed.
    fn set_completed(&mut self);
}

struct Children<Item> {
    /// Number of direct children below this node.
    direct_count: u64,

    /// Total number nodes below this node, recursively.
    total_count: u64,

    /// Expanded children of this item.
    expanded: BTreeMap<u64, ItemNodeRc<Item>>,

    /// Incomplete children of this item.
    incomplete: BTreeMap<u64, ItemNodeWeak<Item>>,
}

impl<Item> Children<Item> {
    fn new(child_count: u64) -> Self {
        Children {
            direct_count: child_count,
            total_count: child_count,
            expanded: BTreeMap::new(),
            incomplete: BTreeMap::new(),
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
    fn expanded(&self, index: u64) -> bool {
        self.expanded.contains_key(&index)
    }

    /// Iterate over the expanded children.
    fn iter_expanded(&self) -> impl Iterator<Item=(&u64, &ItemNodeRc<Item>)> + '_ {
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

    /// Add an incomplete child.
    fn incomplete_add(&mut self, index: u64, child_rc: &ItemNodeRc<Item>) {
        self.incomplete.insert(index, Rc::downgrade(child_rc));
    }

    /// Fetch an incomplete child.
    fn incomplete_fetch(&mut self, index: u64) -> Option<ItemNodeRc<Item>> {
        self.incomplete.get(&index).and_then(Weak::upgrade)
    }

    /// Get the number of rows between two children.
    fn rows_between(&self, start: u64, end: u64) -> u64 {
        (end - start) +
            self.expanded
                .range(start..end)
                .map(|(_, node_rc)| node_rc.borrow().children.total_count)
                .sum::<u64>()
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

    fn expanded(&self) -> bool {
        true
    }

    fn update_total(&mut self, update: &ModelUpdate)
        -> Result<(), ModelError>
    {
        self.children.total_count += update.rows_added;
        self.children.total_count -= update.rows_removed;
        Ok(())
    }

    fn set_completed(&mut self) {}
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

    fn expanded(&self) -> bool {
        match self.parent.upgrade() {
            Some(parent_ref) => parent_ref
                .borrow()
                .children()
                .expanded(self.item_index),
            // Parent is dropped, so node cannot be expanded.
            None => false
        }
    }

    fn update_total(&mut self, update: &ModelUpdate)
        -> Result<(), ModelError>
    {
        self.children.total_count += update.rows_added;
        self.children.total_count -= update.rows_removed;
        self.parent
            .upgrade()
            .ok_or(ModelError::ParentDropped)?
            .borrow_mut()
            .update_total(update)?;
        Ok(())
    }

    fn set_completed(&mut self) {
        if let Some(parent_rc) = self.parent.upgrade() {
            parent_rc
                .borrow_mut()
                .children_mut()
                .incomplete
                .remove(&self.item_index);
        }
    }
}

impl<Item> ItemNode<Item> where Item: Copy {

    pub fn expanded(&self) -> bool {
        Node::<Item>::expanded(self)
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

trait ItemNodeRcOps {
    fn set_expanded(&self, expanded: bool) -> Result<ModelUpdate, ModelError>;
}

impl<Item> ItemNodeRcOps for ItemNodeRc<Item>
where Item: Copy + 'static
{
    fn set_expanded(&self, expanded: bool) -> Result<ModelUpdate, ModelError> {
        // Retrieve information we need from the node.
        let node = self.borrow();
        let parent_rc = node.parent
            .upgrade()
            .ok_or(ModelError::ParentDropped)?;
        let child_count = node.children.direct_count;
        drop(node);

        // Create an empty update that will collect all added/removed rows.
        let mut update = ModelUpdate::default();

        // If collapsing, first recursively collapse the children of this node.
        if !expanded {
            let mut node = self.borrow_mut();
            let expanded_children = node.children.expanded.split_off(&0);
            for (_index, child_rc) in expanded_children {
                // Add the rows removed for this child to the update.
                update += child_rc.set_expanded(false)?;
            }
            // There are no longer any visible incomplete children.
            node.children.incomplete.clear();
        }

        // Add or remove this node from the parent's expanded children.
        parent_rc
            .borrow_mut()
            .children_mut()
            .set_expanded(self, expanded);

        // Add the effect of expanding/collapsing this node by itself.
        let self_update = ModelUpdate {
            rows_added: if expanded { child_count } else { 0 },
            rows_removed: if expanded { 0 } else { child_count },
            rows_changed: 0,
        };

        // Propagate change in total rows back to the root.
        self.borrow_mut().update_total(&self_update)?;

        update += self_update;

        Ok(update)
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

pub struct TreeListModel<Item> {
    capture: Arc<Mutex<Capture>>,
    root: Rc<RefCell<RootNode<Item>>>,
}

pub struct PendingUpdates<Item> {
    position: u64,
    node_rc: AnyNodeRc<Item>,
    start_index: Option<u64>,
    last_index: u64,
    stack: Vec<PendingStackEntry<Item>>,
    to_add: Option<u64>,
    done: bool,
}

struct PendingStackEntry<Item> {
    node_rc: AnyNodeRc<Item>,
    start_index: u64,
    last_index: u64,
}

impl<Item> PendingUpdates<Item> {
    fn pop_stack(&mut self) -> bool {
        if let Some(entry) = self.stack.pop() {
            self.node_rc = entry.node_rc;
            self.start_index = Some(entry.start_index);
            self.last_index = entry.last_index;
            true
        } else {
            false
        }
    }
}

impl<Item> TreeListModel<Item>
where Item: 'static + Copy,
      Capture: ItemSource<Item>
{
    pub fn new(capture: Arc<Mutex<Capture>>) -> Result<Self, ModelError> {
        let mut cap = capture.lock().or(Err(ModelError::LockError))?;
        let item_count = cap.item_count()?;
        Ok(TreeListModel {
            capture: capture.clone(),
            root: Rc::new(RefCell::new(RootNode {
                children: Children::new(item_count),
            })),
        })
    }

    pub fn row_count(&self) -> u64 {
        self.root.borrow().children.total_count
    }

    pub fn set_expanded(&self,
                        node_ref: &ItemNodeRc<Item>,
                        _position: u64,
                        expanded: bool)
        -> Result<ModelUpdate, ModelError>
    {
        if expanded == node_ref.borrow().expanded() {
            return Err(ModelError::AlreadyDone);
        }
        node_ref.set_expanded(expanded)
    }

    pub fn start_updates(&mut self) -> PendingUpdates<Item> {
        println!("");
        PendingUpdates {
            position: 0,
            node_rc: self.root.clone(),
            last_index: 0,
            start_index: Some(0),
            stack: Vec::new(),
            to_add: None,
            done: false,
        }
    }

    pub fn next_update(&mut self,
                       pending: &mut PendingUpdates<Item>)
        -> Result<Option<(u64, ModelUpdate)>, ModelError>
    {
        // Work through the remaining incomplete items, returning an update
        // for any that need it.
        loop {
            println!("Looping");
            // First check if we were already in the middle of updating a node.
            if let Some(children_added) = pending.to_add.take()
            {
                println!("Adding {} pending.to_add", children_added);
                // This node is expanded and has new children to add.
                // The children are added after all the rows of existing ones.
                let end_position = pending.position;
                let children_update = ModelUpdate {
                    rows_added: children_added,
                    rows_removed: 0,
                    rows_changed: 0,
                };

                // Update this node's row count and propagate.
                let mut node = pending.node_rc.borrow_mut();
                let mut children = node.children_mut();
                children.direct_count += children_added;
                node.update_total(&children_update)?;

                // Update the position to continue from.
                pending.position += children_added;
                println!("Advancing to new end at {}", pending.position);

                return Ok(Some((end_position, children_update)));
            }

            // Otherwise if flagged as done, no further updates.
            if pending.done {
                println!("Done");
                return Ok(None);
            }

            match pending.start_index {
                Some(start) => {
                    // Look for the next incomplete child of this node.
                    if let Some((index, child_weak)) = {
                        let next = pending.node_rc
                            .borrow()
                            .children()
                            .incomplete
                            .range(start..)
                            .next()
                            .map(|(i, weak)| (*i, weak.clone()));
                        next
                    } {
                        println!("Found incomplete child at {}", index);
                        if let Some(child_rc) = child_weak.upgrade() {
                            // Found a live and incomplete child.
                            println!("Child is live");

                            // Advance current position up to this child.
                            let node = pending.node_rc.borrow();
                            let children = node.children();
                            pending.position +=
                                children.rows_between(
                                    pending.last_index, index);
                            println!("Advancing to child at {}",
                                     pending.position);
                            pending.last_index = index;
                            drop(node);

                            // Recurse down into this child.
                            pending.stack.push(
                                PendingStackEntry {
                                    node_rc: pending.node_rc.clone(),
                                    start_index: start + 1,
                                    last_index: pending.last_index,
                                }
                            );
                            pending.node_rc = child_rc;
                            pending.start_index = Some(0);
                            pending.last_index = 0;
                        } else {
                            println!("Child is dead");
                            // Child no longer referenced, remove it.
                            pending.node_rc
                                .borrow_mut()
                                .children_mut()
                                .incomplete
                                .remove(&index);
                            pending.start_index = Some(index + 1);
                        }
                    } else {
                        println!("No further children");
                        // No incomplete children left.
                        // Proceed to handling this node itself.
                        pending.start_index = None;
                    }
                }
                None => {
                    println!("Processing node");
                    // Check for updates to this node.
                    let mut node = pending.node_rc.borrow_mut();
                    let item = node.item();
                    let expanded = node.expanded();

                    // Check if this node had new children added.
                    use CompletionStatus::*;
                    let old_child_count = node.children().direct_count;
                    let (completion, new_child_count) = {
                        let mut cap = self.capture.lock()
                            .or(Err(ModelError::LockError))?;
                        match item {
                            Some(item) => cap.children(&item)?,
                            None => (Ongoing, cap.item_count()?)
                        }
                    };
                    let completed = matches!(completion, Complete);
                    let children_added = new_child_count - old_child_count;

                    // If completed, remove from incomplete node list.
                    if completed {
                        node.set_completed();
                    }

                    // If expanded, advance to the end of this node's children.
                    if expanded {
                        let children = node.children_mut();
                        pending.position +=
                            children.rows_between(
                                pending.last_index,
                                children.direct_count);
                        println!("Advancing to end at {}",
                                 pending.position);
                    }

                    if children_added > 0 {
                        println!("{} children added", children_added);
                        // Some children were added. If this node was expanded,
                        // we will need to deal with the added children on the
                        // next loop iteration or call.
                        if item.is_none() {
                            // Root node.
                            println!("Root children pending");
                            // Handle added children on next loop iteration.
                            pending.to_add.replace(children_added);
                            // This will be the last update required.
                            pending.done = true;
                        } else {
                            // Item node. Its own row will be updated.
                            let mut node_position = pending.position - 1;
                            if expanded {
                                println!("Item children pending");
                                // Adjust position for this node's children.
                                node_position -= node.children().total_count;
                                // Add the new children on next call.
                                pending.to_add.replace(children_added);
                            } else {
                                // Update this node's row count.
                                let children = node.children_mut();
                                children.direct_count += children_added;
                                // Done with this node, pop the stack.
                                drop(node);
                                println!("Popping stack");
                                pending.pop_stack();
                            }

                            let node_update = ModelUpdate {
                                rows_added: 0,
                                rows_removed: 0,
                                rows_changed: 1,
                            };
                            println!("Updating item node at {}/{}",
                                     node_position,
                                     self.row_count());
                            return Ok(Some((node_position, node_update)));
                        }
                    } else {
                        // No children were added. Try to pop the stack.
                        drop(node);
                        if !pending.pop_stack() {
                            return Ok(None)
                        }
                    }
                }
            }
        };
    }

    pub fn fetch(&self, position: u64) -> Result<ItemNodeRc<Item>, ModelError> {
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

        // First, check if we already have an incomplete node for this item.
        if let Some(node_rc) = parent_ref
            .borrow_mut()
            .children_mut()
            .incomplete_fetch(relative_position)
        {
            return Ok(node_rc)
        }

        // Otherwise, fetch it from the database.
        let mut cap = self.capture.lock().or(Err(ModelError::LockError))?;
        let parent_item = parent_ref.borrow().item();
        let item = cap.item(&parent_item, relative_position)?;
        let (completion, child_count) = cap.children(&item)?;
        let node = ItemNode {
            item,
            parent: Rc::downgrade(&parent_ref),
            item_index: relative_position,
            children: Children::new(child_count),
        };
        let node_rc = Rc::new(RefCell::new(node));
        if matches!(completion, CompletionStatus::Ongoing) {
            parent_ref
                .borrow_mut()
                .children_mut()
                .incomplete_add(relative_position, &node_rc);
        }
        Ok(node_rc)
    }
}
