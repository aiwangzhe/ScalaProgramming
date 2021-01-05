package reflect

import scala.reflect.ClassTag

object ClassTagTest {
  def main(args: Array[String]): Unit = {
    val tag = ClassTag(List(1, 2).getClass)
    import scala.reflect.runtime.universe._
    val tt = typeTag[Int]
  }
}
