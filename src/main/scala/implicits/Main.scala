package implicits



import scala.reflect.runtime.universe._

object Stringutils {
  implicit class StringImprovement(val s : String){   //隐式类
    def increment = s.map(x => (x +1).toChar)
  }
}

class Test{
  def receive: PartialFunction[Any, Unit] = {
    case Int => print("it is int")
    case _ => print("unknown"￿￿￿)￿
  }
}

object  Main extends  App{

  def getTypeType[T](obj: T, typeTags: TypeTag[T]) = {
    println("type: " + typeTags)
  }

  val t1: Test = new Test


}