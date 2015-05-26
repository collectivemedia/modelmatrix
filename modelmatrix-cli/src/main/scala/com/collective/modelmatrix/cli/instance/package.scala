package com.collective.modelmatrix.cli

import com.bethecoder.ascii_table.ASCIITable
import com.collective.modelmatrix.catalog.{ModelInstance, ModelDefinition}

package object instance {

  def printInstances(instances: Seq[ModelInstance]): Unit = {
    
    val header: Array[String] = Array(
      "Id", "Name", "Created By", "Created At", "Comment", "Features", "Columns"
    )

    val noData: Array[Array[String]] = Array(Array.fill(header.length)("--"))

    val result: Seq[Array[String]] = instances.map { definition =>
      Array(
        definition.id.toString,
        definition.name.getOrElse("n/a"),
        definition.createdBy,
        timeFormatter.format(definition.createdAt),
        definition.comment.getOrElse("n/a"),
        definition.features.toString,
        definition.columns.toString
      )
    }

    Console.out.println(s"Total number of Model Matrix instances: ${instances.length}")
    ASCIITable.getInstance().printTable(header, if (result.nonEmpty) result.toArray else noData)
  }


}
