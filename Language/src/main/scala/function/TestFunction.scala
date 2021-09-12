package function

object TestFunction {

  def main(args: Array[String]): Unit = {
    val data = List(1, 20, 23, 34, 30, 40).toIterator
    val func = (list: Iterator[Int]) => list.size + "."
    val result = count[Int, String](data, func)
    println("result: " + result)
  }

  def count[U, T](data: Iterator[U], func: Iterator[U] => T): T = {
    func(data)
  }
}
