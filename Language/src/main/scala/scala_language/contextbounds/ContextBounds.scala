package scala_language.contextbounds

/** 上下文界定
  * author: leisu
  * date 2018/8/15 17:36
  */
object ContextBounds extends App {
 
  implicit def ageCondition(person: Poet) = person.age > 18
 
  val cb = new ContextBounds
  val list: List[Poet] = List(new Poet("李白", 35),
    new Poet("白居易", 19),
    new Poet("苏轼", 34),
    new Poet("辛弃疾", 17),
    new Poet("杜甫", 21),
    new Poet("张良", 18)
  )
  println(cb.doFilter(list))
}
 
 
class ContextBounds {
 
  /**
    * 根据传入的data和判断条件con，进行数据过滤，并返回
    * 其中T是混入了Condition[T]的子类
    */
  def doFilter[T <: Condition[T]](data: List[T])(implicit condition: T => Boolean): List[T] = data.filter(_.judge(condition))
}
 
 
/**
  * 定义一个Condition特质，包含condition方法，混入此特质的类，可以使用condition进行筛选
  *
  * @tparam T 协变
  */
trait Condition[+T] {
  def judge(condition: T => Boolean): Boolean
}
/**
  * 诗人类
  */
case class Poet(val name: String, val age: Int) extends Condition[Poet] {
  //复写方法，调用con函数，传入this
  override def judge(condition: Poet => Boolean): Boolean = condition(this)
}