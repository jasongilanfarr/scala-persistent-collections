/* ********************************************************
 * Copyright 2012 - Jason Gilanfarr - All Rights Reserved *
 * ********************************************************/
package org.thisamericandream

/**
 *
 */
package object collections {
  implicit def tuple2OptionMap[A](t: (Option[A], Option[A])) = new {
    def map[R](f: A => R): (Option[R], Option[R]) = (t._1.map(f(_)), t._2.map(f(_)))
    def foreach(f: A => Unit) = { map(f) }
  }

  implicit def tuple3OptionMap[A](t: (Option[A], Option[A], Option[A])) = new {
    def map[R](f: A => R): (Option[R], Option[R], Option[R]) = (t._1.map(f(_)), t._2.map(f(_)), t._2.map(f(_)))
    def foreach(f: A => Unit) = { map(f) }
  }
}