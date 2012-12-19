/* ********************************************************
 * Copyright 2012 - Jason Gilanfarr - All Rights Reserved *
 * ********************************************************/
package org.thisamericandream.collections

import scala.annotation.tailrec
import scala.collection.immutable.MapLike
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.MapBuilder
import scala.collection.mutable.Builder
import scala.collection.generic.CanBuildFrom
import scala.unchecked
import org.thisamericandream.collections.mutable.FixedMap

/**
 * Base class for a immutable, copy-on-write B+ Tree.
 *
 * @tparam A key type for the map
 * @tparam B value type for the map
 *
 * <p>
 * Unlike a traditional B+ Tree, there is no infinity pointer, instead
 * the first key points to its left child (which will have the same first key)
 * and the last key points to its last child (which has the same first key as this node's last key)
 * e.g.
 *    BPlusTree(1 -> (1,2),
 *              3 -> (3,4),
 *              5 -> (5,6))
 * </p>
 *
 * <p>
 * Whenever a child node is inserted, all nodes affected by the change will be recreated; however,
 * all children not on the path will stay the same.
 *
 * Note that values are directly inserted when a node is split (to save on extra allocations). This
 * operation is safe as all the split nodes were allocated along the path.
 * </p>
 *
 * <p>
 * TODO: Implement Bulk-Insert
 * TODO: Attempt to remove as much of the type checking warnings from the pattern matching as possible.
 * TODO: Make the leaf and internal nodes truly replacable with alternate implementations (for example,
 *  a Internal node could want Loadable[BPlusTree[Key, Value]] as its children to implement
 *  lazy-loading.
 * </p>
 */
abstract class BPlusTree[A <% Ordered[A], +B]() extends Map[A, B] with MapLike[A, B, BPlusTree[A, B]] {
  /** B or BPlusTree[A, B] */
  type Child

  val nodeSize = children.capacity

  import BPlusTree._

  /**
   * The key of this node is the key of its first child
   *
   */
  final def key: A = if (children.at(0) == null) null.asInstanceOf[A] else children.at(0)._1

  /** Number of active keys in the node */
  private[collections] final def numActiveKeys: Int = children.size
  /** True if and only if this node is a leaf */
  protected[collections] def isLeaf: Boolean
  /** Helper to determine if a node is full */
  private def isFull: Boolean = (numActiveKeys == nodeSize)
  /** Map from Key to the Child type */
  protected[collections] def children: FixedMap[A, Child] = newMap[A, Child](nodeSize)
  /** Minimum Size for a node in the tree */
  private[collections] final def minimumSize: Int = {
    if (isLeaf) {
      nodeSize / 2
    } else
      nodeSize / 2 + nodeSize % 2
  }

  /* Begin: Scala Map[A, B] */
  final def get(key: A): Option[B] = {
    val leaf = findChildPath(this, key).head.asInstanceOf[BPlusTreeLeaf[A, B]]
    leaf.children.get(key)
  }

  override def empty: BPlusTree[A, B] = BPlusTreeLeaf(newMap[A, B](nodeSize))

  final def +[B1 >: B](kv: (A, B1)) = insert(this, kv._1, kv._2)
  final def -(k: A) = delete(this, k)

  final def iterator: Iterator[(A, B)] = new BPlusIterator(this)
  /* End: Scala Map[A, B] */
  private[collections] final def nodeIterator: Iterator[BPlusTree[A, B]] = new BPlusNodeIterator(this)

  /** Pretty print this node in a way more suitable for debugging */
  def prettyPrint(): String = prettyPrint(0)

  /**
   * Pretty print this node in a way more suitable for debugging.
   *
   * @param level current indentation level
   *
   * TODO: Use a StringBuilder.
   */
  def prettyPrint(level: Int): String = this match {
    case leaf: BPlusTreeLeaf[_, _] =>
      ("  " * level) + "{" + numActiveKeys + "} " + leaf.children.mkString("(", ", ", "),\n")
    case internal: BPlusTreeInternal[_, _] =>
      val childString = internal.children.foldLeft("")((a, b) => a + b._2.prettyPrint(level + 1))
      ("  " * level) + "Internal {" + numActiveKeys + "} (" + internal.children.filter(_ != null).map(_._1).mkString(", ") + ") {\n" +
        childString + ("  " * level) + "}\n"
  }

}

/** Default implementation of an internal node. */
protected[collections] case class BPlusTreeInternal[A <% Ordered[A], B](override val children: FixedMap[A, BPlusTree[A, B]]) extends BPlusTree[A, B] {
  type Child = BPlusTree[A, B]

  val isLeaf = false

  override def toString: String = {
    "BTreeInternal(" + numActiveKeys + ", " + children.mkString("[", ", ", "]") + ")"
  }
}

protected[collections] case class BPlusTreeLeaf[A <% Ordered[A], B](override val children: FixedMap[A, B]) extends BPlusTree[A, B] {
  type Child = B

  val isLeaf = true

  override def toString: String = {
    "BTreeLeaf(" + numActiveKeys + ", " + children.mkString("[", ", ", "]") + ")"
  }
}

private class BPlusNodeIterator[A, B](val root: BPlusTree[A, B]) extends Iterator[BPlusTree[A, B]] {
  import BPlusTree._

  var currentSet = List(root)

  def hasNext: Boolean = !currentSet.isEmpty

  def next: BPlusTree[A, B] = {
    if (!currentSet.head.isLeaf) {
      val internal = currentSet.head.asInstanceOf[BPlusTreeInternal[A, B]]
      currentSet = internal.children.values.toList ++ currentSet.tail
      internal
    } else {
      val oldHead = currentSet.head
      currentSet = currentSet.tail
      oldHead
    }
  }
}

/**
 * Iterator over a b-tree
 *
 * @tparam A the key type
 * @tparam B The value type
 */
private class BPlusIterator[A, B](val root: BPlusTree[A, B]) extends Iterator[(A, B)] {
  import BPlusTree._

  var stack = leftMostPath(root)
  var currentIndex = 0
  var lastChild: BPlusTree[A, B] = stack.head
  val rootsLastChild = rightMostChild(root)

  def hasNext: Boolean = if (stack.isEmpty) {
    false
  } else if (lastChild.eq(rootsLastChild) && currentIndex >= rootsLastChild.numActiveKeys) {
    false
  } else {
    true
  }

  def next: (A, B) = {
    @tailrec def nextValue(): (A, B) = stack.head match {
      case leaf: BPlusTreeLeaf[A, B] =>
        lastChild = leaf
        currentIndex += 1
        leaf.children.at(currentIndex - 1)
      case internal: BPlusTreeInternal[A, B] =>
        // need the next child in the internal node, if it exists, if it doesn't, then we need to go to the next
        // internal node up the chain, if its there (stack.tail.head)
        val nextChildIndex = internal.children.indexOfKey(lastChild.key) match {
          case Right(index) => index + 1
          case Left(index) =>
            assume(index == 0)
            index
        }

        if (nextChildIndex < internal.numActiveKeys) {
          currentIndex = 0
          val nextNode = internal.children.at(nextChildIndex)._2
          stack = nextNode :: stack
          nextValue()
        } else {
          lastChild = internal
          stack = stack.tail
          if (stack.isEmpty) {
            throw new UnsupportedOperationException
          }
          nextValue()
        }
    }

    stack.head match {
      case leaf: BPlusTreeLeaf[A, B] =>
        lastChild = leaf
        // we are done with this leaf, move to the next one.
        if (leaf.numActiveKeys <= currentIndex) {
          currentIndex = 0
          stack = stack.tail

          if (stack.isEmpty) {
            // undefined behavior when next is called with hasNext == false
            throw new UnsupportedOperationException
          }

          nextValue()
        } else {
          val kv = leaf.children.at(currentIndex)
          currentIndex += 1
          kv
        }
      case internal: BPlusTreeInternal[_, _] =>
        nextValue()
    }
  }

}

object BPlusTree {
  val defaultSize = 9

  /* Begin: Map[A,B] Builders */
  def apply[A <% Ordered[A], B](kvs: (A, B)*): BPlusTree[A, B] = apply(defaultSize)(kvs: _*)

  def apply[A <% Ordered[A], B](size: Int)(kvs: (A, B)*): BPlusTree[A, B] = {
    // TODO use a bulk insert
    empty(size) ++ kvs
  }

  def newBuilder[A <% Ordered[A], B](size: Int): Builder[(A, B), BPlusTree[A, B]] =
    new MapBuilder[A, B, BPlusTree[A, B]](empty(size))

  implicit def canBuildFrom[A <% Ordered[A], B]: CanBuildFrom[BPlusTree[_, _], (A, B), BPlusTree[A, B]] = {
    new CanBuildFrom[BPlusTree[_, _], (A, B), BPlusTree[A, B]] {
      def apply(from: BPlusTree[_, _]) = newBuilder[A, B](from.nodeSize)
      def apply() = newBuilder[A, B](defaultSize)
    }
  }

  def empty[A <% Ordered[A], B](size: Int = defaultSize): BPlusTree[A, B] = new BPlusTreeLeaf[A, B](new FixedMap[A, B](size))
  /* End: Map[A, B] Builders */

  /** Find the path from the root to the left-most leaf */
  @tailrec private[collections] def leftMostPath[A, B](node: BPlusTree[A, B], path: List[BPlusTree[A, B]] = Nil): List[BPlusTree[A, B]] = {
    node match {
      case leaf: BPlusTreeLeaf[A, B] => leaf :: path
      case internal: BPlusTreeInternal[A, B] => leftMostPath(internal.children.at(0)._2, internal :: path)
    }
  }

  /** Find the right most child of the tree */
  @tailrec private[collections] def rightMostChild[A, B](node: BPlusTree[A, B]): BPlusTreeLeaf[A, B] = node match {
    case leaf: BPlusTreeLeaf[A, B] => leaf
    case internal: BPlusTreeInternal[A, B] =>
      rightMostChild(internal.children.at(internal.numActiveKeys - 1)._2)
  }

  /**
   * Insert a new key-value pair into the tree and returns
   * a new tree with the value inserted.
   *
   * TODO: This code works the path on its own instead of taking advantage
   * of the same methods on the child nodes. Would it be cleaner and/or clearer
   * to call these methods on the children directly?
   * Would it actually be much different?
   */
  private def insert[A <% Ordered[A], B, B1 >: B](root: BPlusTree[A, B], k: A, v: B1): BPlusTree[A, B1] = {
    // path is leaf -> parent *
    val path = findChildPath(root, k)

    val leaf = path.head.asInstanceOf[BPlusTreeLeaf[A, B1]]
    if (leaf.contains(k) || !leaf.isFull) {

      val newChildren = newMap[A, B1](leaf.nodeSize) ++= leaf.children
      newChildren(k) = v

      val insertedLeaf = BPlusTreeLeaf[A, B1](newChildren)
      if (path.tail.isEmpty) {
        insertedLeaf
      } else {
        createTree(path.tail, leaf, insertedLeaf)
      }
    } else {
      val (left, right) = split(leaf, k, v)
      splitPath(path.tail, path.head, left, right)
    }
  }

  /**
   * Recreates the path up the tree when replacing oldChild with newChild
   *
   * @param path The path up the tree to the root, must not be empty.
   * @param oldChild The child that needs to be replaced.
   * @param newChild The replacement child.
   *
   * @return The newly created path from oldChild through root. All nodes on the path are recreated.
   */
  @tailrec def createTree[A <% Ordered[A], B, B1 >: B](path: List[BPlusTree[A, B1]],
                                                       oldChild: BPlusTree[A, B],
                                                       newChild: BPlusTree[A, B1]): BPlusTree[A, B1] = path match {
    case Nil => newChild
    case (internal: BPlusTreeInternal[A, B]) :: tail =>
      val key = newChild match {
        case leaf: BPlusTreeLeaf[_, _] => leaf.children.head._1
        case internal: BPlusTreeInternal[_, _] => internal.children.head._1
      }

      val newChildren = newMap[A, BPlusTree[A, B1]](internal.nodeSize) ++= internal.children

      val oldKey = oldChild.key

      newChildren -= oldChild.key
      newChildren(key) = newChild

      val insertedInternal = BPlusTreeInternal[A, B1](newChildren)

      if (path.tail.isEmpty) {
        return insertedInternal
      } else {
        createTree(path.tail, internal, insertedInternal)
      }
  }

  /**
   * Recursively split the head of the tree if it is full replacing originalNode
   * with the new left and right trees
   *
   * @path originalNode The original node left and right were created from.
   * @parm left The new left sub-tree
   * @param right The new right sub-tree
   *
   * @returns the last split node and the remaining path
   */
  @tailrec private def splitPath[A <% Ordered[A], B, B1 >: B](path: List[BPlusTree[A, B]],
                                                              originalNode: BPlusTree[A, B],
                                                              left: BPlusTree[A, B1],
                                                              right: BPlusTree[A, B1]): BPlusTree[A, B1] = {

    if (path.isEmpty) {
      val children = newMap[A, BPlusTree[A, B1]](originalNode.nodeSize)

      children(left.key) = left
      children(right.key) = right

      BPlusTreeInternal[A, B1](children)
    } else if (path.head.isFull) {
      val (newParentLeft, newParentRight) = split(path.head.asInstanceOf[BPlusTreeInternal[A, B]],
        left,
        right).asInstanceOf[(BPlusTreeInternal[A, B1], BPlusTreeInternal[A, B1])]

      splitPath(path.tail, path.head, newParentLeft, newParentRight)
    } else {
      val newChildren = newMap[A, BPlusTree[A, B1]](originalNode.nodeSize)
      newChildren ++= path.head.asInstanceOf[BPlusTreeInternal[A, B]].children
      newChildren -= originalNode.key
      newChildren(left.key) = left
      newChildren(right.key) = right

      val newParent = BPlusTreeInternal[A, B1](newChildren)
      if (path.head.eq(this) || path.isEmpty || path.tail.isEmpty) {
        newParent
      } else {
        createTree(path.tail, path.head, newParent)
      }
    }

  }

  /**
   * Recurse down the tree (using binary search) for leaf node the given key exists or would exist at.
   *
   * @param tree The tree to search the key for.
   * @param key The key to search for.
   *
   * @return The path to the leaf node from <b>this</b> where the key belongs.
   */
  private[collections] def findChildPath[A <% Ordered[A], B, B1 >: B](tree: BPlusTree[A, B1], key: A): List[BPlusTree[A, B1]] = {
    @tailrec def findChild[B1 >: B](tree: BPlusTree[A, B1], key: A, path: List[BPlusTree[A, B1]]): List[BPlusTree[A, B1]] = tree match {
      case leaf: BPlusTreeLeaf[_, _] =>
        leaf :: path
      case internal: BPlusTreeInternal[A, B] =>
        val index = internal.children.indexOfKey(key)

        val leaf = index match {
          case Right(index) => internal.children.at(index)._2
          case Left(index) =>
            if (index - 1 >= 0 && (internal.children.at(index) == null || internal.children.at(index)._1 > key)) {
              internal.children.at(index - 1)._2
            } else {
              internal.children.at(index)._2
            }
        }

        findChild(leaf, key, internal :: path)
    }
    findChild(tree, key, Nil)
  }

  /**
   * Split the requested leaf node in half, inserting the key-value pair into the appropriate leaf
   *
   * @param node The node to split.
   * @param k The key to insert into the appropriate leaf
   * @param v The value to associate with the key.
   *
   * @return (left, right) of the node given.
   */
  private def split[A <% Ordered[A], B, B1 >: B](leaf: BPlusTreeLeaf[A, B1], k: A, v: B1): (BPlusTreeLeaf[A, B1], BPlusTreeLeaf[A, B1]) = {

    val midPoint = leaf.minimumSize
    val midPointKey = leaf.children.at(midPoint)._1
    val splitAt = if (k >= midPointKey) { midPoint + 1 } else { midPoint }

    val (leftChildren, rightChildren) = leaf.children.split[B1](splitAt)

    if (leftChildren.size <= rightChildren.size) {
      assume(k < rightChildren.at(0)._1)
      leftChildren(k) = v
    } else {
      rightChildren(k) = v
    }
    (BPlusTreeLeaf[A, B1](leftChildren), BPlusTreeLeaf[A, B1](rightChildren))
  }

  /**
   * Split the requested internal node in half, inserting the newLeft and newRight subtrees into the appropriate internal node
   *
   * @param node The node to split.
   * @param newLeft The newLeft subtree from a child splitting
   * @param newRight The newRight subtree from a child splitting
   *
   * @return (left, right) of the node given.
   */
  private def split[A <% Ordered[A], B, B1 >: B](internal: BPlusTreeInternal[A, B],
                                                 newLeft: BPlusTree[A, B1] = null,
                                                 newRight: BPlusTree[A, B1] = null): (BPlusTreeInternal[A, B1], BPlusTreeInternal[A, B1]) = {

    val midPoint = internal.minimumSize
    val midPointKey = internal.children.at(midPoint - 1)._1
    val splitAt = if (newLeft.key >= midPointKey) {
      midPoint
    } else {
      midPoint - 1
    }

    val (leftChildren, rightChildren) = internal.children.split[BPlusTree[A, B1]](splitAt)

    if (newLeft.key < rightChildren.at(0)._1) {
      // the new left subtree is replacing an existing key
      leftChildren.indexOfKey(newLeft.key) match {
        case Right(index) =>
          leftChildren(newLeft.key) = newLeft
        case Left(index) =>
          leftChildren.updateAt(index, (newLeft.key, newLeft))
      }
    } else {
      // the new left subtree should be replacing an existing key
      rightChildren.indexOfKey(newLeft.key) match {
        case Right(index) =>
          rightChildren(newLeft.key) = newLeft
        case Left(index) =>
          rightChildren.updateAt(index, (newLeft.key, newLeft))
      }
    }

    // new right is always a newly added item, so we can just insert it.
    if (newRight.key < midPointKey) {
      leftChildren(newRight.key) = newRight
    } else {
      rightChildren(newRight.key) = newRight
    }
    (BPlusTreeInternal[A, B1](leftChildren), BPlusTreeInternal[A, B1](rightChildren))
  }

  /**
   * Delete a given key from the tree.
   *
   * @param root The root of the tree.
   * @param key The key to remove.
   *
   * @return The new tree with the key removed.
   *
   * <p>
   * Start at root, find leaf L where entry belongs.
   * Remove the entry.
   * If L is at least half-full, done.
   * If L has fewer entries than it should,
   * Try to re-distribute, borrowing from siblings (adjacent node with same parent as L).
   * If re-distribution fails, merge L and siblings.
   * If merge occurred, must delete entry (pointing to L or sibling) from parent of L.
   * Merge could propagate to root, decreasing height.
   * </p>
   */
  private def delete[A <% Ordered[A], B](root: BPlusTree[A, B], key: A): BPlusTree[A, B] = {
    val path = findChildPath(root, key)
    val leaf = path.head.asInstanceOf[BPlusTreeLeaf[A, B]]
    if (!leaf.children.contains(key)) {
      root
    } else if (leaf.numActiveKeys - 1 >= leaf.minimumSize || path.tail.isEmpty) {
      createTree(path.tail, path.head, BPlusTreeLeaf[A, B](leaf.children - key))
    } else {
      val parent = path.tail.head.asInstanceOf[BPlusTreeInternal[A, B]]
      val (leftSibling, rightSibling) =
        getSiblings(leaf, parent).map(_.asInstanceOf[BPlusTreeLeaf[A, B]])

      val rebalanced = rebalance(leftSibling.map(_.children), leaf.children - key, rightSibling.map(_.children), leaf.minimumSize)

      val newParent = parent.children - leaf.key
      leftSibling.foreach(newParent -= _.key)
      rightSibling.foreach(newParent -= _.key)

      // rebalance worked.
      if (rebalanced.isDefined) {
        // TODO: newLeft and newRight might be the same children as the originals.
        val (newLeft, newMiddle, newRight) = rebalanced.get

        newLeft.map(BPlusTreeLeaf[A, B](_)).foreach(x => newParent += (x.key -> x))
        newRight.map(BPlusTreeLeaf[A, B](_)).foreach(x => newParent += (x.key -> x))
        newParent += (newMiddle.at(0)._1 -> BPlusTreeLeaf[A, B](newMiddle))

        rebalanceTree(path.tail, newParent)
      } else {
        // rebalance failed, so we need to merge the leaf and its siblings.
        val (newLeft, newRight) = merge(leftSibling.map(_.children), leaf.children - key, rightSibling.map(_.children), leaf.minimumSize)
        newParent += (newLeft.at(0)._1 -> BPlusTreeLeaf[A, B](newLeft))

        newRight.map(BPlusTreeLeaf[A, B](_)).foreach(x => newParent += (x.key -> x))

        rebalanceTree(path.tail, newParent)
      }
    }
  }

  /**
   * Rebalance the internal nodes going up the tree if necessary. Basically the same
   * as delete; however, this is made only for internal nodes due to the types of the children.
   *
   * @param path The path up to the root.
   * @param children The new children of the path's head.
   *
   * @return the rebalanced tree.
   */
  @tailrec private def rebalanceTree[A <% Ordered[A], B](path: List[BPlusTree[A, B]], children: FixedMap[A, BPlusTree[A, B]]): BPlusTree[A, B] = {
    if (path.isEmpty || path.tail.isEmpty) {
      if (children.size == 1) {
        // root collapsed.
        children.at(0)._2
      } else {
        BPlusTreeInternal[A, B](children)
      }
    } else if (children.size >= path.head.minimumSize) {
      createTree(path.tail, path.head, BPlusTreeInternal[A, B](children))
    } else {
      val internal = path.head.asInstanceOf[BPlusTreeInternal[A, B]]
      val parent = path.tail.head.asInstanceOf[BPlusTreeInternal[A, B]]

      val (leftSibling, rightSibling) =
        getSiblings(internal, parent).map(_.asInstanceOf[BPlusTreeInternal[A, B]])

      val rebalanced = rebalance(leftSibling.map(_.children), children, rightSibling.map(_.children), parent.minimumSize)
      val newParent = parent.children - internal.key
      leftSibling.foreach(newParent -= _.key)
      rightSibling.foreach(newParent -= _.key)

      if (rebalanced.isDefined) {
        val (newLeft, newMiddle, newRight) = rebalanced.get

        newLeft.map(BPlusTreeInternal[A, B](_)).foreach(x => newParent += (x.key -> x))
        newRight.map(BPlusTreeInternal[A, B](_)).foreach(x => newParent += (x.key -> x))
        newParent += (newMiddle.at(0)._1 -> BPlusTreeInternal[A, B](newMiddle))

        rebalanceTree(path.tail, newParent)
      } else {
        val (newLeft, newRight) = merge(leftSibling.map(_.children), children, rightSibling.map(_.children), parent.minimumSize)
        newParent += (newLeft.at(0)._1 -> BPlusTreeInternal[A, B](newLeft))
        newRight.map(BPlusTreeInternal[A, B](_)).foreach(x => newParent += (x.key -> x))

        rebalanceTree(path.tail, newParent)
      }
    }
  }

  /**
   * Get the siblings of the given node.
   * @param node The node to find the siblings of
   * @param parent The nodes immediate parent.
   * @return The right and left siblings of the given node in the immediate parent.
   */
  private def getSiblings[A <% Ordered[A]](node: BPlusTree[A, _],
                                           parent: BPlusTreeInternal[A, _]): (Option[BPlusTree[A, _]], Option[BPlusTree[A, _]]) = {

    val nodeIndex = parent.children.indexOfKey(node.key).right.get

    if (nodeIndex > 0 && nodeIndex < parent.numActiveKeys - 1) {
      (Some(parent.children.at(nodeIndex - 1)._2), Some(parent.children.at(nodeIndex + 1)._2))
    } else if (nodeIndex == 0) {
      (None, Some(parent.children.at(nodeIndex + 1)._2))
    } else {
      (Some(parent.children.at(nodeIndex - 1)._2), None)
    }
  }

  /**
   * Rebalance the middle children with their left and right siblings such that
   * the siblings will maintain at least their minimum size.
   *
   * @param left The left sibling.
   * @param middle The node needing rebalancing.
   * @param right The right sibling.
   * @param minimumSize The minimum size of the array.
   */
  def rebalance[A <% Ordered[A], B](left: Option[FixedMap[A, B]],
                                    middle: FixedMap[A, B],
                                    right: Option[FixedMap[A, B]], minimumSize: Int): Option[(Option[FixedMap[A, B]], FixedMap[A, B], Option[FixedMap[A, B]])] = {

    assume(left.isDefined || right.isDefined)

    // TODO detect if rebalancing is possible such that middle will have at least minimumSize elements.
    if (left.map(_.size - 1).getOrElse(0) < minimumSize && right.map(_.size - 1).getOrElse(0) < minimumSize) {
      None
    } else {
      val (newLeft, newMiddleLeft) = left.map { l =>
        if (l.size - 1 >= minimumSize) {
          val (newLeft, newMiddle) = l.split(minimumSize)
          newMiddle ++= middle
          (Some(newLeft), newMiddle)
        } else {
          (Some(l), middle)
        }
      }.getOrElse((None, middle))

      if (newMiddleLeft.size >= minimumSize) {
        Some((newLeft, newMiddleLeft, right))
      } else {
        val (newMiddle, newRight) = right.map { r =>
          if (r.size - 1 >= minimumSize) {
            val (m, newRight) = r.split(r.size - minimumSize)
            m ++= newMiddleLeft
            (m, Some(newRight))
          } else {
            (newMiddleLeft, Some(r))
          }
        }.getOrElse((newMiddleLeft, None))

        // TODO can we detect this earlier?
        if (newMiddle.size >= minimumSize) {
          Some((newLeft, newMiddle, newRight))
        } else {
          None
        }
      }
    }
  }

  /**
   * Merge up to three maps into 1 or 2 maps such that each map is of the minimumSize.
   *
   * @param left The left sibling
   * @param middle The node that needs to be merged.
   * @param right The right sibling
   * @param minimumSize The minimumSize of the maps
   *
   * @return The merged map and a second map if the merge would have overflowed.
   */
  private def merge[A <% Ordered[A], B](left: Option[FixedMap[A, B]],
                                        middle: FixedMap[A, B],
                                        right: Option[FixedMap[A, B]],
                                        minimumSize: Int): (FixedMap[A, B], Option[FixedMap[A, B]]) = {

    if (left.map(_.size).getOrElse(0) + middle.size + right.map(_.size).getOrElse(0) <= middle.capacity) {
      val elements = middle.clone
      left.foreach(elements ++= _)
      right.foreach(elements ++= _)
      (elements, None)
    } else {
      val (l, r) = (newMap[A, B](middle.capacity), newMap[A, B](middle.capacity))

      left.foreach(l ++= _)
      val leftRemaining = minimumSize - l.size
      l ++= middle.take(leftRemaining)
      r ++= middle.drop(leftRemaining)
      right.foreach(r ++= _)
      (l, Some(r))
    }
  }

  def newMap[A <% Ordered[A], B](size: Int): FixedMap[A, B] = new FixedMap[A, B](size)
}