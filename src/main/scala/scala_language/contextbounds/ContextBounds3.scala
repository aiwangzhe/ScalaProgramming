package scala_language.contextbounds3

/**
  * 封装原方法的类，可以看到这里有一个类型参数U，并且我们的方法带上类型参数，可用度更加广了
  */
abstract class MyFunctionClass[U] {
  val myFunction: (U => Boolean)
}

/** 上下文界定
  * author: leisu
  * date 2018/8/15 17:36
  */
object ContextBounds3 extends App {

  /*XXX 改动一： 这里封装了下原先的方法，把它放到了MyFunctionClass类之下
  * 因为上下文引入必须是带有一个类型参数的类的实例
  * 虽然原先的方法时Function1[Poet, Boolean]的实例，但无法再给它加其他类型参数，所以封装*/
  implicit val ageCondition: MyFunctionClass[Poet] = new MyFunctionClass[Poet] {
    val myFunction: (Poet => Boolean) = _.age > 18
  };

  val cb = new ContextBounds3
  val list: List[Poet] = List(new Poet("李白", 35),
    new Poet("白居易", 19),
    new Poet("苏轼", 34),
    new Poet("辛弃疾", 17),
    new Poet("杜甫", 21),
    new Poet("张良", 18)
  )
  println("完全上下界实现：" + cb.doFilter(list))
}


class ContextBounds3 {

  /**
    * 根据传入的data和判断条件con，进行数据过滤，并返回
    * 这里类型参数变为： [T: MyFunctionClass] 有两个意思：
    * 1. 引入类一个类型参数T，并且这个类型参数与 MyFunctionClass无直接关联（不同于上界下界，需要是后面的父类或者子类）
    * 2. 自动为此方法添加了一个MyFunctionClass[T]的隐式入参，也就是上面object中定义的：implicit val ageCondition
    */
  def doFilter[T: MyFunctionClass](data: List[T]): List[T] = data.filter(implicitly[MyFunctionClass[T]].myFunction(_)) //XXX 注意这里的改动
}


/**
  * 定义一个Condition特质，包含condition方法，混入此特质的类，可以使用condition进行筛选
  *
  * @tparam T 协变
  */
trait Condition[T] {
  def judge(condition: T => Boolean): Boolean
}
/**
  * 诗人类
  */
case class Poet(val name: String, val age: Int) extends Condition[Poet] {
  //复写方法，调用con函数，传入this
  override def judge(condition: Poet => Boolean): Boolean = condition(this)
}
