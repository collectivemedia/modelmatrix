package com.collective.modelmatrix

import java.time.ZoneId
import java.time.format.{FormatStyle, DateTimeFormatter}
import java.util.Locale

import com.bethecoder.ascii_table.{ASCIITableHeader, ASCIITable}
import com.collective.modelmatrix.transform.{Index, Top, Identity, Transform}

import scala.language.implicitConversions

package object cli {

  val timeFormatter =
    DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
      .withLocale(Locale.US)
      .withZone(ZoneId.systemDefault())

  implicit def stringToASCIITableHeader(s: String): ASCIITableHeader =
    new ASCIITableHeader(s)

  implicit class StringOps(val s: String) extends AnyVal {
    def dataLeftAligned: ASCIITableHeader = new ASCIITableHeader(s, -1)
  }

  import scalaz.stream._
  import scalaz.concurrent.Task

  implicit class ConcurrentProcess[O](val process: Process[Task, O]) {
    def concurrently[O2](concurrencyLevel: Int)
        (f: Channel[Task, O, O2]): Process[Task, O2] = {
      val actions =
        process.
          zipWith(f)((data, f) => f(data))

      val nestedActions =
        actions.map(Process.eval)

      merge.mergeN(concurrencyLevel)(nestedActions)
    }
  }

}
