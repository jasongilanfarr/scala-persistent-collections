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
class BPlusTreeSpec extends WordSpec with ShouldMatchers {
  import BPlusTree._

  val defaultSize = 3

  "BPlusTrees" when {
    "empty" should {
      val empty = BPlusTree.empty[Int, Int]

      "claim to be empty" in {
        empty should be('empty)
      }
      "return a new tree when a new value is added" in {
        val newTree = empty + (1 -> 1)
        newTree should not be ('eq(empty))
      }
    }
    "it has 1 element" should {
      val oneElement = BPlusTree(1 -> 1)
      "not be empty" in { oneElement should not be ('empty) }
      "have size 1" in { oneElement.size should be(1) }
      "head should be the element" in { oneElement.head should equal((1, 1)) }
      "should contain the element" in { oneElement.get(1) should equal(Some(1)) }
    }

    "run through the original test set" should {

      val valueSet = Seq(
        Seq(2, 3, 1, 9, 6, 7, 4, 8, 0, 5),
        Seq(4, 7, 6, 1, 5, 5, 0, 3),
        Seq(5, 9, 3, 7, 4, 2, 8, 1, 6, 10),
        Seq(9, 8, 7, 6, 5, 4, 3, 2, 1, 0),
        Seq(7, 8, 9, 5, 0, 1, 6, 4, 3, 2),
        Seq(8, 6, 0, 10, 2, 4, 7, 3, 1, 5))

      valueSet.foreach { sequence =>
        "have all the values and iterate in sorted order for: " + sequence in {
          val root = BPlusTree[Int, Int](sequence.map(x => (x -> x)): _*)
          sequence.filterNot(x => root.contains(x)) should be('empty)
          root.toList.map(_._1) == root.toList.map(_._1).sortWith(_ < _)
        }
      }
    }

    "given a large amount of data in random order" should {
      // large list of random values matched to themselves.
      val seq = Random.shuffle(0.until(100000))
      val values = seq.map { x => (x, x) }
      val root = BPlusTree(values: _*)

      val rootSequence = root.toSeq.map(_._1)
      "iterate the data in exactly sorted order" in {
        rootSequence should equal(rootSequence.sortWith(_ < _))

      }
      "not have any duplicates" in {
        val duplicateEntries = rootSequence.groupBy(identity)
          .filter(x => x._2.size > 1)
          .map(_._1).toSeq.sortWith(_ < _)

        duplicateEntries should be('empty)
      }
      "contain all the entries inserted" in {
        seq.filter(!root.contains(_)) should be('empty)
      }
      "have equidistant paths to all children" in {
        val pathLengths = seq.map(x => (x, root.findChildPath(root, x).size))
        pathLengths.groupBy(_._2).keys.size should equal(1)
      }
    }

  }
}