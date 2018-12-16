package conversions

import scala.io.StdIn

object AnyToAny {

  def main(args: Array[String]): Unit = {
    var sn = StdIn.readInt()
    var sb = StdIn.readInt()
    var db = StdIn.readInt()

    var m = 1
    var dec = 0
    var dn = 0
    while ( sn != 0 ) {
      dec = dec + (sn % 10) * m
      m *= sb
      sn /= 10
    }
    m = 1
    while ( dec != 0 ) {
      dn = dn + (dec % db) * m
      m *= 10
      dec /= db
    }

    println(dn)
  }
}
