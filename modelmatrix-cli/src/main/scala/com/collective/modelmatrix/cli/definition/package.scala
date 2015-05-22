package com.collective.modelmatrix.cli

import java.time.ZoneId
import java.time.format.{FormatStyle, DateTimeFormatter}
import java.util.Locale

import com.bethecoder.ascii_table.{ASCIITable, ASCIITableHeader}
import com.collective.modelmatrix.catalog.ModelDefinition
import com.collective.modelmatrix.transform.{Transform, Identity, Top, Index}

import scala.language.implicitConversions

package object definition {

  val timeFormatter =
    DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
      .withLocale(Locale.US)
      .withZone(ZoneId.systemDefault())

  def printSeparator(n: Int = 3): Unit = {
    Console.out.print(Seq.fill(n)(System.lineSeparator()).mkString(""))
  }

  def printParameters(t: Transform): String = t match {
    case Identity => ""
    case Top(p, ao) => s"percentage = $p; allOther = $ao"
    case Index(p, ao) => s"percentage = $p; allOther = $ao"
  }

  def printDefinitions(definitions: Seq[ModelDefinition]): Unit = {
    val header: Array[String] = Array(
      "Id", "Name", "Created By", "Created At", "Comment", "Features"
    )

    val noData: Array[Array[String]] = Array(Array.fill(header.length)("--"))

    val result = definitions.map { definition =>
      Array(
        definition.id.toString,
        definition.name.getOrElse("n/a"),
        definition.createdBy,
        timeFormatter.format(definition.createdAt),
        definition.comment.getOrElse("n/a"),
        definition.features.toString
      )
    }

    Console.out.println(s"Total number of Model Matrix definitions: ${definitions.length}")
    ASCIITable.getInstance().printTable(header, if (result.nonEmpty) result.toArray else noData)
  }

  implicit def stringToASCIITableHeader(s: String): ASCIITableHeader =
    new ASCIITableHeader(s)

  implicit class StringOps(val s: String) extends AnyVal {
    def dataLeftAligned: ASCIITableHeader = new ASCIITableHeader(s, -1)
  }

}
