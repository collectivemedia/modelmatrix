package com.collective.modelmatrix.cli

import org.scalatest.FlatSpec

class SinkSpec extends FlatSpec {

  "Sink" should "parse csv sink" in {
    val sink = "csv://file://temp/data/output.csv"
    val parsed = Sink(sink)
    assert(parsed == CsvSink("file://temp/data/output.csv"))
  }

  it should "parse hive sink" in {
    val sink = "hive://mm.featurized"
    val parsed = Sink(sink)
    assert(parsed == HiveSink("mm.featurized"))
  }

}
