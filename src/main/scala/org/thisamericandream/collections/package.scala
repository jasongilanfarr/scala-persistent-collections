/* ********************************************************
 * Copyright 2012 - Jason Gilanfarr - All Rights Reserved *
 * ********************************************************/
package org.thisamericandream
import language.implicitConversions

/**
 *
 */
package object collections {
  implicit class RichTuple2[A](t: (A, A)) extends Tuple2[A, A](t._1, t._2) {
    def map[R](f: A => R): (R, R) = (f(t._1), f(t._2))
    def foreach(f: A => Unit) = { map(f) }
  }
  
  implicit class RichTuple3[A](t: (A, A, A)) extends Tuple3[A, A, A](t._1, t._2, t._3) {
    def map[R](f: A => R): (R, R, R) = (f(t._1), f(t._2), f(t._3))
    def foreach(f: A => Unit) = { map(f) }
  }
}