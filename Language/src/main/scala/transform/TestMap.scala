package transform

object TestMap {

  def main(args: Array[String]): Unit = {
    (1 to 5).toList map intToString
  }

  def intToString(i : Int): Unit = {
    (i + 2).toString
  }
}
