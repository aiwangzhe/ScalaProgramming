package basictypesandoperations


object TestList {

  def main(args: Array[String]): Unit = {
    val oneTwo = List(1, 2)
    val threeFour = List(3, 4)
    val oneTwoAndThreeFour = oneTwo:::threeFour
    println("oneTwo: " + oneTwo)
    println("threeFour: " + threeFour)
    println("oneTwoAndThreeFour: " + oneTwoAndThreeFour)

    val twoThree = List(2, 3)
    //val onwTwoThree = 1 :: twoThree
    //println(onwTwoThree)
    val oneTwoThree = 1 :: 2 :: 3 :: Nil
    println(oneTwoThree)
  }
}
