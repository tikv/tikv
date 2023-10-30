// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, io::Write, marker::PhantomData};

pub use indextree::{Arena, NodeId};
use smallvec::SmallVec;
use tracing::span;
use tracing_subscriber::{
    registry::{LookupSpan, SpanRef},
    Registry,
};

use super::data::Data;

/// The visitor of the tree. We will perform a depth-first searching over the
/// tree. It has the same behavior of [`NodeId::traverse`].
pub trait TreeVisit {
    type Error;

    /// Executed while we are entering a new node.
    fn enter(
        &mut self,
        tree: &Arena<span::Id>,
        node: NodeId,
        span: MaybeSpan<'_>,
    ) -> Result<(), Self::Error>;

    /// Executed while we are go back from the current node, go to its parent.
    fn leave(&mut self, tree: &Arena<span::Id>, node: NodeId) -> Result<(), Self::Error>;
}

pub enum MaybeSpan<'a> {
    Span(SpanRef<'a, Registry>),
    Dropped(span::Id),
}

impl<'a> MaybeSpan<'a> {
    pub fn span(self) -> Option<SpanRef<'a, Registry>> {
        match self {
            MaybeSpan::Span(span) => Some(span),
            MaybeSpan::Dropped(_) => None,
        }
    }
}

/// A tree equipped with a pointer tracing the current frame.
/// This tree contains the spans which presents the "frame"s of one execution.
/// This tree is non-empty. (it seems empty execution is no meaning?)
pub struct Tree {
    arena: Arena<span::Id>,
    root: NodeId,
    current: NodeId,
}

impl std::fmt::Debug for Tree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree")
            .field("root", &self.root)
            .field("current", &self.current)
            .field("root_span", &self.arena[self.root].get())
            .field("current_span", &self.arena[self.current].get())
            .finish()
    }
}

impl Tree {
    pub fn new_with_root(rt: span::Id) -> Self {
        let mut arena = Arena::new();
        let root = arena.new_node(rt);
        Tree {
            arena,
            root,
            current: root,
        }
    }

    /// Check whether the tree is currently on the root.
    pub fn on_root(&self) -> bool {
        self.current == self.root
    }

    /// Check whether the tree only contains the root node.
    pub fn only_root(&self) -> bool {
        self.root.children(&self.arena).next().is_none()
    }

    /// Append a frame to the current node's child.
    pub fn step_in(&mut self, frame: &span::Id) {
        match self
            .current
            .children(&self.arena)
            .find(|c| self.arena[*c].get() == frame)
        {
            Some(i) => {
                self.current = i;
            }
            None => {
                let new_node = self.arena.new_node(frame.clone());
                self.current.append(new_node, &mut self.arena);
                self.current = new_node;
            }
        }
    }

    /// Move out the current stack. (i.e. go to the caller)
    ///
    /// # Panics
    ///
    /// It may panic when `self.on_root() == true`.
    pub fn step_out(&mut self) {
        let c = self.current;
        let p = self.arena[c].parent();
        debug_assert!(p.is_some(), "step out on root");
        if let Some(p) = p {
            self.current = p
        }
    }

    /// Remove a child and its subtree of the current frame by its span ID.
    ///
    /// # Returns
    ///
    /// Whether some subtree has been removed.
    pub fn remove_child(&mut self, id: &span::Id) -> bool {
        if let Some(child) = self
            .current
            .children(&self.arena)
            .find(|child| self.arena[*child].get() == id)
        {
            child.remove_subtree(&mut self.arena);
            true
        } else {
            false
        }
    }

    /// Traverse the tree with a [`TreeVisit`].
    /// This function must be called at the context where the layer has been
    /// registered. i.e. the [`Registry`] of this tree must be the default
    /// subscriber. Also notice don't use this in `get_default` context.
    pub fn traverse_with<E, V: TreeVisit<Error = E>>(&self, mut visit: V) -> Result<(), E> {
        tracing::dispatcher::get_default(|def| {
            let lookup = def.downcast_ref::<Registry>();

            let iter = self.root.traverse(&self.arena);
            for event in iter {
                match event {
                    indextree::NodeEdge::Start(node) => {
                        let id = self.arena[node].get();
                        let span = lookup.and_then(|l| l.span(id));
                        visit.enter(
                            &self.arena,
                            node,
                            span.map(MaybeSpan::Span)
                                .unwrap_or(MaybeSpan::Dropped(id.clone())),
                        )?;
                    }
                    indextree::NodeEdge::End(node) => visit.leave(&self.arena, node)?,
                }
            }
            Ok(())
        })
    }

    /// Format the tree to a human readable format.
    /// Returns a UTF-8 byte string.
    pub fn fmt_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        self.traverse_with(FormatTreeTo::ascii(&mut res))
            .expect("failed to dump bytes to a vector");
        res
    }

    /// Format the tree to a human readable format.
    pub fn fmt_string(&self) -> String {
        let res = self.fmt_bytes();
        match String::from_utf8_lossy(&res) {
            // SAFETY: `from_utf8_lossy` returns the origin string, it must be a valid string.
            Cow::Borrowed(_) => unsafe { String::from_utf8_unchecked(res) },
            Cow::Owned(rep) => rep,
        }
    }

    pub fn current_span(&self) -> &span::Id {
        self.arena[self.current].get()
    }
}

fn format(mut w: impl Write, span: MaybeSpan<'_>) -> std::io::Result<()> {
    match span {
        MaybeSpan::Span(span) => {
            writeln!(
                w,
                "[{}:{}] [span_id={}] [{:?}] {}",
                span.metadata().file().unwrap_or("UNKNOWN"),
                span.metadata().line().unwrap_or(0),
                span.id().into_u64(),
                span.name(),
                span.extensions().get::<Data>().unwrap()
            )?;
        }
        MaybeSpan::Dropped(id) => {
            writeln!(w, "[DROPPED] [span_id={}]", id.into_u64())?;
        }
    }
    Ok(())
}

pub struct FormatPlainTo<W> {
    forked_indices: SmallVec<[u32; 8]>,
    current_idx: u32,

    output: W,
}

impl<W> FormatPlainTo<W> {
    pub fn new(output: W) -> Self {
        Self {
            current_idx: 0,
            forked_indices: SmallVec::new(),
            output,
        }
    }
}

impl<W: Write> TreeVisit for FormatPlainTo<W> {
    type Error = std::io::Error;

    fn enter(
        &mut self,
        tree: &Arena<span::Id>,
        node: NodeId,
        span: MaybeSpan<'_>,
    ) -> std::io::Result<()> {
        self.current_idx += 1;

        write!(self.output, "[")?;
        for i in self.forked_indices.iter() {
            write!(self.output, "{i}& ")?;
        }
        write!(self.output, "{}] ", self.current_idx)?;
        format(&mut self.output, span)?;
        let forked = node.children(tree).nth(1).is_some();
        if forked {
            self.forked_indices.push(self.current_idx);
            self.current_idx = 0;
        }

        Ok(())
    }

    fn leave(&mut self, _tree: &Arena<span::Id>, _node: NodeId) -> std::io::Result<()> {
        if self.current_idx == 0 {
            self.current_idx = self.forked_indices.pop().unwrap_or(0);
        }
        self.current_idx -= 1;
        Ok(())
    }
}

pub trait Style {
    const BRANCH: char;
    const SPACE: char;
    const FORKED_TIP: char;
    const TIP: char;
}

pub struct UnicodeTree;

impl Style for UnicodeTree {
    const BRANCH: char = '│';
    const SPACE: char = ' ';
    const FORKED_TIP: char = '├';
    const TIP: char = '└';
}

pub struct AsciiTree;

impl Style for AsciiTree {
    const BRANCH: char = '|';
    const SPACE: char = ' ';
    const FORKED_TIP: char = '+';
    const TIP: char = '`';
}

pub struct FormatTreeTo<W, S: Style> {
    indent_str: String,

    output: W,
    _style: PhantomData<S>,
}

impl<W> FormatTreeTo<W, AsciiTree> {
    pub fn ascii(output: W) -> Self {
        FormatTreeTo::stylized(output)
    }
}

impl<W> FormatTreeTo<W, UnicodeTree> {
    pub fn unicode(output: W) -> Self {
        FormatTreeTo::stylized(output)
    }
}

impl<W, S: Style> FormatTreeTo<W, S> {
    pub fn stylized(output: W) -> Self {
        Self {
            indent_str: String::new(),
            output,
            _style: PhantomData,
        }
    }
}

impl<W: Write, S: Style> TreeVisit for FormatTreeTo<W, S> {
    type Error = std::io::Error;

    fn enter(
        &mut self,
        tree: &Arena<span::Id>,
        node: NodeId,
        span: MaybeSpan<'_>,
    ) -> std::io::Result<()> {
        let not_last_one = tree[node].next_sibling().is_some();
        let tip_ch = if not_last_one { S::FORKED_TIP } else { S::TIP };
        write!(self.output, "{}{}", self.indent_str, tip_ch)?;
        format(&mut self.output, span)?;

        let branch_ch = if not_last_one { S::BRANCH } else { S::SPACE };
        self.indent_str.push(branch_ch);
        Ok(())
    }

    fn leave(&mut self, _tree: &Arena<span::Id>, _node: NodeId) -> std::io::Result<()> {
        self.indent_str.pop();
        Ok(())
    }
}
