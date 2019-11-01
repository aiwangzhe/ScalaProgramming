package conversions

import java.util
import java.util.{Arrays, HashSet, InputMismatchException}

import scala.io.StdIn

object AnyBaseToAnyBase {
  // Smallest and largest base you want to accept as valid input
  private[conversions] val MINIMUM_BASE = 2
  private[conversions] val MAXIMUM_BASE = 36

  def main(args: Array[String]): Unit = {
    var n: String = null
    var b1 = 0
    var b2 = 0
    var continue = true
    while (continue) {
      try {
        println("Enter number: ")
        n = StdIn.readLine()
        print("Enter beginning base (between " + MINIMUM_BASE + " and " + MAXIMUM_BASE + "): ")
        b1 = StdIn.readInt()
        if (b1 > MAXIMUM_BASE || b1 < MINIMUM_BASE) {
          System.out.println("Invalid base!")
        }else {
          if (!validForBase(n, b1)) {
            System.out.println("The number is invalid for this base!")
          }else{
            print("Enter end base (between "+MINIMUM_BASE+" and "+MAXIMUM_BASE+"): ");
            b2 = StdIn.readInt()
            if (b2 > MAXIMUM_BASE || b2 < MINIMUM_BASE) {
              println("Invalid base!");
            }else{
              continue = false
            }
          }
        }
      }catch {
        case e: InputMismatchException => {
          StdIn.readLine()
        }
      }
    }

    println(base2base(n, b1, b2))
  }

  /**
    * Checks if a number (as a String) is valid for a given base.
    */
  def validForBase(n: String, base: Int): Boolean = {
    val validDigits: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')
    // digitsForBase contains all the valid digits for the base given
    val digitsForBase: Array[Char] = util.Arrays.copyOfRange(validDigits, 0, base)
    // Convert character array into set for convenience of contains() method
    val digitsList: util.HashSet[Character] = new util.HashSet[Character]
    for (i <- 0 until digitsForBase.length){
      digitsList.add(digitsForBase(i));
    }
    // Check that every digit in n is within the list of valid digits for that base.
    for (c <- n.toCharArray) {
      if (!digitsList.contains(c)) return false
    }
    true
  }

  /**
    * Method to convert any integer from base b1 to base b2. Works by converting from b1 to decimal,
    * then decimal to b2.
    *
    * @param n  The integer to be converted.
    * @param b1 Beginning base.
    * @param b2 End base.
    * @return n in base b2.
    */
  def base2base(n: String, b1: Int, b2: Int): String = { // Declare variables: decimal value of n,
    // character of base b1, character of base b2,
    // and the string that will be returned.
    var decimalValue = 0
    var charB2 = 0
    var charB1 = 0
    var output = ""
    // Go through every character of n
    for (i <- 0 until n.length) { // store the character in charB1
      charB1 = n.charAt(i)
      // if it is a non-number, convert it to a decimal value >9 and store it in charB2
      charB2 = if (charB1 >= 'A' && charB1 <= 'Z') 10 + (charB1 - 'A')
                  else charB1 - '0'

      // Convert the digit to decimal and add it to the
      // decimalValue of n
      decimalValue = decimalValue * b1 + charB2
    }
    // Converting the decimal value to base b2:
    // A number is converted from decimal to another base
    // by continuously dividing by the base and recording
    // the remainder until the quotient is zero. The number in the
    // new base is the remainders, with the last remainder
    // being the left-most digit.
    // While the quotient is NOT zero:
    while (decimalValue != 0) { // If the remainder is a digit < 10, simply add it to
      // the left side of the new number.
      output = if (decimalValue % b2 < 10) Integer.toString(decimalValue % b2) + output
                  else  ((decimalValue % b2) + 55).toChar + output
      // Divide by the new base again
      decimalValue /= b2
    }
    output
  }

}
