/* ********************************************************
 * Copyright 2012 - Jason Gilanfarr - All Rights Reserved *
 * ********************************************************/
package org.thisamericandream.collections

import scala.collection.mutable.ArrayBuffer
import org.scalatest.matchers.ShouldMatchers
import scala.util.Random
import org.scalatest.WordSpec

/**
 *
 */
class BPlusTreeSpec extends WordSpec with ShouldMatchers with BPlusTreeBehaviors {
  import BPlusTree._

  val RANDOM_SIZE = 10000

  val defaultSize = 3

  "BPlusTrees" when {
    "empty" should {
      val empty = BPlusTree[Int, Int]()
      "claim to be empty" in {
        empty should be('empty)
      }
      "return a new tree when a new value is added" in {
        val newTree = empty + (1 -> 1)
        newTree should not be ('eq(empty))
      }
    }
    "given 1 element" should {
      val oneElement = BPlusTree(1 -> 1)
      "not be empty" in { oneElement should not be ('empty) }
      "have size 1" in { oneElement.size should be(1) }
      "head should be the element" in { oneElement.head should equal((1, 1)) }
      "should contain the element" in { oneElement.get(1) should equal(Some(1)) }
    }
    "given a small amount of data in a random order" should {
      val seq = Random.shuffle(0.until(33))

      "given " + seq + " in a small, odd sized tree" should {
        behave like nonEmptyTree(seq, 3)
      }
      "given " + seq + " in a small, evenly sized tree" should {
        behave like nonEmptyTree(seq, 4)
      }
      "given " + seq + " in a medium, oddly sized tree" should {
        behave like nonEmptyTree(seq, 9)
      }
      "given " + seq + " in a medium, evenly sized tree" should {
        behave like nonEmptyTree(seq, 16)
      }
    }

    "given a large amount of data in a random order" should {
      val seq = Random.shuffle(0.until(RANDOM_SIZE))

      "in a small, odd sized tree" should {
        behave like nonEmptyTree(seq, 3)
      }
      "in a small, evenly sized tree" should {
        behave like nonEmptyTree(seq, 4)
      }
      "in a medium, oddly sized tree" should {
        behave like nonEmptyTree(seq, 97)
      }
      "in a medium, evenly sized tree" should {
        behave like nonEmptyTree(seq, 100)
      }
      "in a large, oddly sized tree" should {
        behave like nonEmptyTree(seq, 511)
      }
      "in a large, evenly sized tree" should {
        behave like nonEmptyTree(seq, 512)
      }
    }
  }
}

trait BPlusTreeBehaviors extends ShouldMatchers {
  this: WordSpec =>

  import BPlusTree._

  def nonEmptyTree[A <% Ordered[A]](sequence: => Seq[A], size: Int, treeMaybe: => Option[BPlusTree[A, A]] = None) {
    val tree = treeMaybe.getOrElse(BPlusTree[A, A](size)(sequence.map(x => (x, x)): _*))
    val treeEntries = tree.toSeq.map(_._1)

    "have all the values and iterate in sorted order" in {
      sequence.filterNot(x => tree.contains(x)) should be('empty)
      tree.toList.map(_._1) == tree.toList.map(_._1).sortWith(_ < _)
    }

    "not have any duplicates" in {
      val duplicateEntries = treeEntries.groupBy(identity)
        .filter(x => x._2.size > 1)
        .map(_._1).toSeq.sortWith(_ < _)

      duplicateEntries should be('empty)
    }
    "contain all the entries inserted" in {
      sequence.filter(!tree.contains(_)) should be('empty)
    }
    "have equidistant paths to all children" in {
      val pathLengths = sequence.map(x => (x, findChildPath(tree, x).size))
      pathLengths.groupBy(_._2).keys.size should equal(1)
    }
    "have all nodes meet their minimum size" in {
      tree.nodeIterator.foreach { node =>
        if (node.eq(tree)) {
          (node.numActiveKeys >= 2) should be(true)
        } else {
          (node.numActiveKeys >= node.minimumSize) should be(true)
        }
      }
    }
  }
}
