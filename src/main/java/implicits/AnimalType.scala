package implicits

class SwingType{
  def  wantLearned(sw : String) = println("兔子已经学会了"+sw)
}
object swimming{
  implicit def learningType(s : AnimalType) = new SwingType
}
class AnimalType
object AnimalType extends  App{
  import swimming._
  val rabbit = new AnimalType
  rabbit.wantLearned("breaststroke")         //蛙泳
}
