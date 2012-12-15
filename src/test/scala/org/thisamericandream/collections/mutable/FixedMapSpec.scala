/* ********************************************************
 * Copyright 2012 - Jason Gilanfarr - All Rights Reserved *
 * ********************************************************/
package org.thisamericandream.collections.mutable

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.WordSpec

class FixedMapSpec extends WordSpec with ShouldMatchers {
  "FixedMaps" when {
    "empty" should {
      val empty = FixedMap.empty[Int, String]

      "claim to be empty" in {
        empty should be('empty)
      }

      "allow a value to be inserted" in {
        empty(5) = "5"
        empty(5) should equal("5")
      }
    }
    "partially full" should {
      val values = Seq(3, 1, 5)
      val fm = FixedMap[Int, Int](size = values.size * 2)(values.map(x => (x, x)): _*)

      "not be empty" in {
        values should not be ('empty)
      }
      "have size " + values.size in {
        fm.size should equal(values.size)
      }
      "contain exactly the same entries as inserted" in {
        fm.filter(x => !values.contains(x._1)) should be('empty)
        values.filter(x => !fm.contains(x)) should be('empty)
      }
      "be able to look up each key using get" in {
        fm.get(3) should equal(Some(3))
        fm.get(1) should equal(Some(1))
        fm.get(5) should equal(Some(5))
      }
      "not be able to find a key that isn't there" in {
        fm.get(2) should equal(None)
      }
      /* TODO Refactor these into a fixture */
      "be able to insert at the beginning" in {
        val clone = fm.clone
        clone eq fm should be(false)
        clone(0) = 0
        clone.get(0) should equal(Some(0))
        clone(0) should equal(0)
        clone.at(0) should equal(0, 0)
        clone.size should equal(4)
        clone.array.size should equal(fm.capacity)
        clone.contains(0) should be(true)
        values.filter(x => !clone.contains(x)) should be('empty)
      }
      "be able to insert in the middle" in {
        val clone = fm.clone
        clone(2) = 2
        clone.get(2) should equal(Some(2))
        clone(2) should equal(2)
        clone.at(1) should equal(2, 2)
        values.filter(x => !clone.contains(x)) should be('empty)
      }
      "be able to insert at the end" in {
        val clone = fm.clone
        clone(6) = 6
        clone.get(6) should be(Some(6))
        clone(6) should equal(6)
        clone.at(3) should equal(6, 6)
        values.filter(x => !fm.contains(x)) should be('empty)
      }
      "be able to remove the first key" in {
        val clone = fm.clone
        clone -= 1
        clone.get(1) should be(None)
        clone.at(0) should be(3, 3)
      }
      "be able to remove the middle key" in {
        val clone = fm.clone
        clone -= 3
        clone.get(3) should be(None)
        clone.at(1) should be(5, 5)

      }
      "be able to remove the last key" in {
        val clone = fm.clone
        clone -= 5
        clone.get(5) should be(None)
        values.filter(x => x != 5 && !fm.contains(x)) should be('empty)
      }
      "report the correct size after adding a key" in {
        val clone = fm.clone
        val oldSize = clone.size
        oldSize should equal(3)
        clone(2) = 2
        clone.size should equal(oldSize + 1)
      }
      "report the correct size after removing a key" in {
        val clone = fm.clone
        val oldSize = clone.size
        oldSize should equal(3)
        clone -= 1
        clone.size should equal(oldSize - 1)
      }
      "be able to split in the middle" in {
        val split = fm.split(fm.size / 2)
        val defaultSplit = fm.splitAt(fm.size / 2)
        // FixedMap.split is covariant, while splitAt(index) is invariant and is
        // defined in GenTraversableLike
        split should equal(defaultSplit)
      }
      "be able to merge with a nearly empty fixed map" in {
        val nearlyEmpty = FixedMap[Int, Int](size = fm.capacity)((2, 2))
        val (left, right) = nearlyEmpty.rebalanceWith(fm)
        right should be(None)

        left should equal((fm ++ nearlyEmpty))
      }
    }
    "be able to rebalance with a nearly half-full fixed map" in {
      val left = FixedMap[Int, Int](size = 6)((2 -> 2), (3 -> 3))
      val right = FixedMap[Int, Int](size = 6)((4 -> 4), (5 -> 5), (6 -> 6), (7 -> 7))
      val (newLeft, newRight) = left.rebalanceWith(right)

      newLeft.keySet should equal(Set(2, 3, 4))
      newRight should be('defined)
      newRight.get.keySet should equal(Set(5, 6, 7))

    }
    "when full" should {
      val values = List(1, 3, 5, 7, 9)
      val fm = FixedMap[Int, Int](size = 5)(values.map(x => (x, x)): _*)

      "be able to remove a middle key" in {
        val clone = fm.clone
        clone -= 5
        clone.get(5) should be(None)
      }
      "be able to remove the last key" in {
        val clone = fm.clone
        clone -= 9
        clone.get(9) should be(None)
        clone.array(clone.array.length - 1) should be(null)
      }
      "be able to remove the first key" in {
        val clone = fm.clone
        clone -= 1
        clone.get(1) should be(None)
        clone.array(0) should be(3, 3)
      }
      "be able to replace a key with a new value" in {
        val clone = fm.clone
        clone(3) = 5
        clone.get(3) should equal(Some(5))
        clone(3) should be(5)
        clone.array should equal(Array((1, 1), (3, 5), (5, 5), (7, 7), (9, 9)))
      }

    }
  }
}