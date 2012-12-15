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
 * TODO: Make nodeSize a parameter.
 * TODO: Implement Bulk-Insert
 * TODO: Implement Delete
 * TODO: Attempt to remove as much of the type checking warnings from the pattern matching as possible.
 * TODO: Dramatically expand the test suite which currently tests some small trees (which are good tests with nodeSize == 3)
 *       followed by a fairly large tree inserted in random order.
 * TODO: Make the leaf and internal nodes truly replacable with alternate implementations (for example,
 *  a Internal node could want Loadable[BPlusTree[Key, Value]] as its children to implement
 *  lazy-loading.
 * </p>
 */
abstract class BPlusTree[A <% Ordered[A], +B]() extends Map[A, B] with MapLike[A, B, BPlusTree[A, B]] {
  /** B or BPlusTree[A, B] */
  type Child

  import BPlusTree._

  /** TODO: Make nodeSize configurable per-tree */
  require(children.capacity == nodeSize)

  /**
   * The key of this node is the key of its first child
   *
   */
  final def key: A = if (children.at(0) == null) null.asInstanceOf[A] else children.at(0)._1

  /** Number of active keys in the node */
  private[collections] final def numActiveKeys: Int = children.size
  /** True if and only if this node is a leaf */
  protected def isLeaf: Boolean
  /** Helper to determine if a node is full */
  private def isFull: Boolean = (numActiveKeys == nodeSize)
  /** Map from Key to the Child type */
  protected[collections] def children = newMap[Child]

  /* Begin: Scala Map[A, B] */
  final def get(key: A): Option[B] = {
    val leaf = findChildPath(this, key).head.asInstanceOf[BPlusTreeLeaf[A, B]]
    leaf.children.get(key)
  }

  override def empty: BPlusTree[A, B] = BPlusTreeLeaf(newMap[B])

  final def +[B1 >: B](kv: (A, B1)) = insert(kv._1, kv._2)
  final def -(k: A) = delete(k)

  final def iterator: Iterator[(A, B)] = new BPlusIterator(this)
  /* End: Scala Map[A, B] */

  /**
   * Insert a new key-value pair into the tree and returns
   * a new tree with the value inserted.
   *
   * TODO: This code works the path on its own instead of taking advantage
   * of the same methods on the child nodes. Would it be cleaner and/or clearer
   * to call these methods on the children directly?
   * Would it actually be much different?
   */
  final def insert[B1 >: B](k: A, v: B1): BPlusTree[A, B1] = {
    // path is leaf -> parent *
    val path = findChildPath(this, k)

    val leaf = path.head.asInstanceOf[BPlusTreeLeaf[A, B1]]
    if (leaf.contains(k) || !leaf.isFull) {

      val newChildren = newMap[B1] ++= leaf.children
      newChildren(k) = v

      val insertedLeaf = BPlusTreeLeaf[A, B1](newChildren)
      if (path.tail.isEmpty) {
        insertedLeaf
      } else {
        createTree(path.tail, leaf, insertedLeaf)
      }
    } else {
      if (path.tail.isEmpty) {
        // splitting root
        val (left, right) = split(path.head, k, v)
        splitPath(Nil, path.head, left, right)
      } else {
        val (left, right) = split(path.head, k, v)
        splitPath(path.tail, path.head, left, right)
      }
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
  @tailrec private def createTree[B1 >: B](path: List[BPlusTree[A, B1]],
                                           oldChild: BPlusTree[A, B1],
                                           newChild: BPlusTree[A, B1]): BPlusTreeInternal[A, B1] = path.head match {
    case internal: BPlusTreeInternal[A, B] =>
      val key = newChild match {
        case leaf: BPlusTreeLeaf[_, _] => leaf.children.head._1
        case internal: BPlusTreeInternal[_, _] => internal.children.head._1
      }

      val newChildren = newMap[BPlusTree[A, B1]] ++= internal.children

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
   * returns the last split node and the remaining path
   *
   */
  @tailrec private def splitPath[B1 >: B](path: List[BPlusTree[A, B1]],
                                          originalNode: BPlusTree[A, B1],
                                          left: BPlusTree[A, B1],
                                          right: BPlusTree[A, B1]): BPlusTree[A, B1] = {

    if (path.isEmpty) {
      val children = newMap[BPlusTree[A, B1]]

      children(left.key) = left
      children(right.key) = right

      BPlusTreeInternal[A, B1](children)
    } else if (path.head.isFull) {
      val (newParentLeft, newParentRight) = split(path.head, newLeft = left, newRight = right).asInstanceOf[(BPlusTreeInternal[A, B1], BPlusTreeInternal[A, B1])]

      splitPath(path.tail, path.head, newParentLeft, newParentRight)
    } else {
      val newChildren = newMap[BPlusTree[A, B1]]
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
  private[collections] final def findChildPath[B1 >: B](tree: BPlusTree[A, B1], key: A): List[BPlusTree[A, B1]] = {
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
   * Split the requested node in half, inserting the (key, value) pair into the new leafs if the node is a leaf,
   * and the newLeft and newRight subtrees if the node is an internal node.
   *
   * @param node The node to split.
   * @param k The key to insert into the new leaf (must be specified if the node is a leaf)
   * @param v The value to insert into the new leaf (must be specified if the node is a leaf, can be null)
   * @param newLeft The newLeft subtree from a child splitting (must be specified if the node is an internal node)
   * @param newRight The newRight subtree from a child splitting (must be specified if the node is an internal node)
   *
   * @return (left, right) of the node given.
   */
  private def split[B1 >: B](node: BPlusTree[A, B1],
                             k: A = null.asInstanceOf[A],
                             v: B1 = null.asInstanceOf[B1],
                             newLeft: BPlusTree[A, B1] = null,
                             newRight: BPlusTree[A, B1] = null): (BPlusTree[A, B1], BPlusTree[A, B1]) = {

    node match {
      case leaf: BPlusTreeLeaf[_, B1] =>
        require(k != null)
        val (leftChildren, rightChildren) = leaf.children.split[B1](nodeSize / 2)

        if (k < rightChildren.at(0)._1) {
          leftChildren(k) = v
        } else {
          rightChildren(k) = v
        }

        (BPlusTreeLeaf[A, B1](leftChildren), BPlusTreeLeaf[A, B1](rightChildren))
      case internal: BPlusTreeInternal[A, B1] =>
        require(newLeft != null && newRight != null)
        val (leftChildren, rightChildren) = internal.children.split[BPlusTree[A, B1]](nodeSize / 2)

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
        if (newRight.key < rightChildren.at(0)._1) {
          leftChildren(newRight.key) = newRight
        } else {
          rightChildren(newRight.key) = newRight
        }
        (BPlusTreeInternal[A, B1](leftChildren), BPlusTreeInternal[A, B1](rightChildren))
    }
  }

  // TODO: Implement Delete.
  def delete(key: A): BPlusTree[A, B] = {
    this
  }

  private def newMap[B]: FixedMap[A, B] = new FixedMap[A, B](nodeSize)

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

/**
 * Iterator over a b-tree
 *
 * @tparam A the key type
 * @tparam B The value type
 */
private class BPlusIterator[A, B](val root: BPlusTree[A, B]) extends Iterator[(A, B)] {
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
  /** Find the path from the root to the left-most leaf */
  @tailrec private def leftMostPath(node: BPlusTree[A, B], path: List[BPlusTree[A, B]] = Nil): List[BPlusTree[A, B]] = {
    node match {
      case leaf: BPlusTreeLeaf[A, B] => leaf :: path
      case internal: BPlusTreeInternal[A, B] => leftMostPath(internal.children.at(0)._2, internal :: path)
    }
  }

  /** Find the right most child of the tree */
  @tailrec private def rightMostChild(node: BPlusTree[A, B]): BPlusTreeLeaf[A, B] = node match {
    case leaf: BPlusTreeLeaf[A, B] => leaf
    case internal: BPlusTreeInternal[A, B] =>
      rightMostChild(internal.children.at(internal.numActiveKeys - 1)._2)
  }
}

object BPlusTree {
  val nodeSize = 9

  def apply[A <% Ordered[A], B](k: A, v: B): BPlusTree[A, B] = {
    val children = new FixedMap[A, B](nodeSize)
    children(k) = v
    new BPlusTreeLeaf[A, B](children)
  }

  /* Begin: Map[A,B] Builders */
  def apply[A <% Ordered[A], B](kvs: (A, B)*): BPlusTree[A, B] = {
    // TODO use a bulk insert
    val initialRoot = BPlusTree(kvs.head._1, kvs.head._2)
    kvs.tail.foldLeft(initialRoot) { (root, kv) =>
      val newRoot = root.insert(kv._1, kv._2)
      newRoot
    }
  }

  def newBuilder[A <% Ordered[A], B]: Builder[(A, B), BPlusTree[A, B]] =
    new MapBuilder[A, B, BPlusTree[A, B]](empty)

  implicit def canBuildFrom[A <% Ordered[A], B]: CanBuildFrom[BPlusTree[_, _], (A, B), BPlusTree[A, B]] = {
    new CanBuildFrom[BPlusTree[_, _], (A, B), BPlusTree[A, B]] {
      def apply(from: BPlusTree[_, _]) = newBuilder[A, B]
      def apply() = newBuilder[A, B]
    }
  }

  def empty[A <% Ordered[A], B]: BPlusTree[A, B] = new BPlusTreeLeaf[A, B](new FixedMap[A, B](nodeSize))
  /* End: Map[A, B] Builders */
}