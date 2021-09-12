package variances

abstract class Animal {
  def name: String
}
case class Cat(name: String) extends Animal
case class Dog(name: String) extends Animal

object InvarianceTest {

  def printAnimalNames(animals: List[Animal]): Unit =
    animals.foreach {
      animal => println(animal.name)
    }

  def main(args: Array[String]): Unit = {
    val cats: List[Cat] = List(Cat("Whiskers"), Cat("Tom"))
    val dogs: List[Dog] = List(Dog("Fido"), Dog("Rex"))

    // prints: Whiskers, Tom
    printAnimalNames(cats)

    // prints: Fido, Rex
    printAnimalNames(dogs)
  }
}