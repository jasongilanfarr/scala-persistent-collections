/* ********************************************************
 * Copyright 2012 - Jason Gilanfarr - All Rights Reserved *
 * ********************************************************/
package org.thisamericandream.collections.mutable

import scala.collection.mutable.Map
import scala.collection.mutable.MapLike
import scala.annotation.tailrec

/**
 * A fixed-sized mutable map backed by an array of tuples.
 *
 * @tparam A the type of keys in the map
 * @tparam B the type of values associated with the keys
 */
class FixedMap[A <% Ordered[A], B](initialSize: Int)
    extends Map[A, B]
    with MapLike[A, B, FixedMap[A, B]] {

  private[collections] val array = new Array[(A, B)](initialSize)
  /** The current size of the map */
  private var size0: Int = 0

  def get(key: A): Option[B] = {
    indexOfKey(key) match {
      case Left(index) => None
      case Right(index) => Some(array(index)._2)
    }
  }

  /** The capacity of the map */
  val capacity = initialSize
  override def size = size0

  override def empty = new FixedMap(array.length)

  def iterator: Iterator[(A, B)] = array.view.filter(_ != null).iterator

  def +=(kv: (A, B)): this.type = {
    binarySearch(kv._1) match {
      case Left(index) =>
        if (index > array.length) {
          throw new ArrayIndexOutOfBoundsException(index)
        } else {
          insertAt(index, kv)
          size0 += 1
        }
      case Right(index) => array(index) = (kv._1, kv._2)
    }
    this
  }

  def -=(key: A): this.type = {
    binarySearch(key) match {
      case Left(index) =>
      case Right(index) =>
        remove(index)
        size0 -= 1
    }
    this
  }

  /**
   * Run a binary search through the array for a given key.
   *
   * @param k The key to search for
   * @return Left(index) if the key is not in the map. The index reflects the position
   *    in the array the key would be found.
   * @return Right(index) if the key is in the map. The index reflects the position
   *    in the array the key was found.
   */
  private[collections] def binarySearch(k: A): Either[Int, Int] = {
    @tailrec def recurse(low: Int, high: Int): Either[Int, Int] = {
      // if the first key is null, then insert at head.
      if (array(0) == null) return Left(0)

      val mid = (low + ((high - low) / 2))

      if (mid > array.length || low > high) return Left(math.min(array.length - 1, mid))
      if (array(mid) == null) return Left(mid)

      array(mid)._1 match {
        case x if x > k =>
          recurse(low, mid - 1)
        case x if x == k =>
          Right(mid)
        case x if x < k =>
          recurse(mid + 1, high)
      }
    }

    val high = {
      val lastNull = array.indexWhere(_ == null)
      if (lastNull == -1) {
        array.length - 1
      } else {
        lastNull
      }
    }
    recurse(0, high)
  }

  /** Remove the value at the specified index */
  private def remove(index: Int) {
    require(index < array.length && index >= 0)
    if (index == 0) {
      System.arraycopy(array, 1, array, 0, array.length - 1)
      array(array.length - 1) = null
    } else if (index == array.length - 1) {
      array(index) = null
    } else {
      System.arraycopy(array, 0, array, 0, index)
      System.arraycopy(array, index + 1, array, index, array.length - index - 1)
      // the last value will always be removed.
      array(array.length - 1) = null
    }
  }

  /** Insert the key-value pair at the specified index */
  private def insertAt(index: Int, kv: (A, B)) {
    assume(index < array.length && index >= 0)
    if (index == 0) {
      System.arraycopy(array, 0, array, 1, array.length - 1)
      array(0) = kv
    } else if (index == array.length - 1) {
      array(index) = kv
    } else {
      System.arraycopy(array, 0, array, 0, index)
      System.arraycopy(array, index, array, index + 1, array.length - index - 1)
      array(index) = kv
    }
  }

  /**
   * Run a binary search through the array for a given key.
   *
   * @param k The key to search for
   * @return Left(index) if the key is not in the map. The index reflects the position
   *    in the array the key would be found.
   * @return Right(index) if the key is in the map. The index reflects the position
   *    in the array the key was found.
   */
  def indexOfKey(k: A): Either[Int, Int] = binarySearch(k)
  /** @return the key at the specified index, if any */
  def at(index: Int) = array(index)
  /**
   * Update the specified index with a new key value pair.
   */
  def updateAt(index: Int, kv: (A, B)) = array.update(index, kv)

  /**
   * Split the map at the specified index into two covariant fixed maps.
   */
  def split[B1 >: B](index: Int): (FixedMap[A, B1], FixedMap[A, B1]) = {
    assume(index != 0 && index < size0 - 1)
    val (left, right) = (new FixedMap[A, B1](capacity), new FixedMap[A, B1](capacity))
    var i = 0
    while (i < size) {
      (if (i < index) left else right) += array(i)
      i += 1
    }

    (left, right)
  }

  /**
   * Rebalance this map with another map such that either
   *  this map is merged into the other map (if there is sufficient capacity)
   *  or this map will steal up to half the other map's first entries so that
   *  this map will remain at least half-full.
   *
   * <b>The maps must have the same capacity.</b>
   *
   * @return if the maps were merged, a new map with all of the entries and None for the second value of the tuple
   * @return if the maps were simply rebalanced, two new maps with even division across them and the lesser keyed
   * map first.
   * @return (this, Some(other)) if this map was at least half-full.
   */
  def rebalanceWith(other: FixedMap[A, B]): (FixedMap[A, B], Option[FixedMap[A, B]]) = {
    assume(capacity == other.capacity)
    if (size >= capacity / 2) {
      (this, Some(other))
    } else if (other.size <= other.capacity / 2 && size + other.size <= other.capacity) {
      val merged = new FixedMap[A, B](other.capacity)
      merged ++= other
      merged ++= this
      (merged, None)
    } else {
      if (at(0)._1 < other.at(0)._1) {
        val (newThis, newOther) = other.split(other.size / 2 - 1)
        newThis ++= this
        (newThis, Some(newOther))
      } else {
        val (newOther, newThis) = other.split(other.size / 2 + 1)
        newThis ++= this
        (newOther, Some(newThis))
      }
    }

  }
  override def toString: String = {
    array.mkString("(", ", ", ")")
  }
}

object FixedMap {
  val defaultSize = 32

  def apply[A <% Ordered[A], B](kv: (A, B)*): FixedMap[A, B] = {
    new FixedMap[A, B](kv.size) ++= kv

  }
  def apply[A <% Ordered[A], B](size: Int)(kv: (A, B)*): FixedMap[A, B] = {
    new FixedMap[A, B](size) ++= kv
  }

  def empty[A <% Ordered[A], B]: FixedMap[A, B] = new FixedMap(defaultSize)

}