package com.collective.modelmatrix.cli

import org.scalatest.FlatSpec

class SourceSpec extends FlatSpec {

  "Source" should "parse csv source" in {
    val source = "csv://file://temp/data/input.csv"
    val parsed = Source(source)
    assert(parsed == CsvSource("file://temp/data/input.csv"))
  }

}
