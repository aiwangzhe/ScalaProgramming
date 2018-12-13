package implicits

object ImplictConvert {

  implicit def intToString(x : Int) = x.toString

  def main(args: Array[String]): Unit = {
    foo(10)
  }

  def person(implicit name: String) = name

  def foo(msg : String) = println(msg)
}
