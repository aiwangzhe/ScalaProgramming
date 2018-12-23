/*
 * Copyright (C) 2007-2008 Artima, Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Example code from:
 *
 * Programming in Scala (First Edition, Version 6)
 * by Martin Odersky, Lex Spoon, Bill Venners
 *
 * http://booksites.artima.com/programming_in_scala
 */
package functional.ex6


class Rational(val n: Int, val d: Int) {
//  require(d != 0)
//  val numer: Int = n
//  val denom: Int = d
  override def toString = n +"/"+ d
//  def add(that: Rational): Rational =
//    new Rational(
//      numer * that.denom + that.numer * denom,
//      denom * that.denom
//    )

  def merge(that: Rational): Rational = {
    new Rational(n + that.n, d + that.d)
  }

//  def lessThan(that: Rational) =
//    this.numer * that.denom < that.numer * this.denom
//
//  def max(that: Rational) =
//    if (this.lessThan(that)) that else this
}

object Main {
  def main(args: Array[String]) {
    val oneHalf = new Rational(1, 2)
    val twoThirds = new Rational(2, 3)

    println("oneHalf [" + oneHalf + "]")
    println("twoThirds [" + twoThirds + "]")
//
//    println("oneHalf.lessThan(twoThirds) [" + oneHalf.lessThan(twoThirds) + "]")
//    println("oneHalf.max(twoThirds) [" + oneHalf.max(twoThirds) + "]")
  }
}
