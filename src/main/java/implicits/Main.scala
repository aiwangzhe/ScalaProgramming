package implicits

object Stringutils {
  implicit class StringImprovement(val s : String){   //隐式类
    def increment = s.map(x => (x +1).toChar)
  }
}
object  Main extends  App{
   import Stringutils._
  println("mobin".increment)
}