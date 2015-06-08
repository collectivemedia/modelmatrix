package com.collective.modelmatrix.transform

import org.scalatest.{ShouldMatchers, FlatSpec}

class BinnerSpec extends FlatSpec with ShouldMatchers with Binner {

  "Binner" should "get from diff to original values" in {
    val diff = Seq(0.1, 0.21, 0.05, 0.5)
    assert(fromDiff(diff) == Seq(0.1, 0.31, 0.36, 0.86))
  }

  it should "get diff from original values" in {
    val values = Seq(0.1, 0.31, 0.37, 0.88)
    assert(toDiff(values) == Seq(0.1, 0.21, 0.06, 0.51))
  }

  it should "calculate perfect split of 10 bins" in {
    val x = (0 to 100).toArray.map(_.toDouble)

    val split = optimalSplit(x, 10, 0, 0)
    split.foreach { s =>
     s.count should be ((x.length / 10) +- 5)
    }
    assert(split.length == 10)
    assert(split.map(_.count).sum == x.length)
  }

  it should "calculate perfect split of 2 bins" in {
    val x = (0 to 100).toArray.map(_.toDouble)

    val split = optimalSplit(x, 2, 0, 0)
    split.foreach { s =>
      s.count should be ((x.length / 2) +- 5)
    }
    assert(split.length == 2)
    assert(split.map(_.count).sum == x.length)
  }

  it should "calculate perfect split of 3 bins" in {
    val x = (0 to 100).toArray.map(_.toDouble)

    val split = optimalSplit(x, 3, 0, 0)
    split.foreach { s =>
      s.count should be ((x.length / 3) +- 5)
    }
    assert(split.length == 3)
    assert(split.map(_.count).sum == x.length)
  }


  it should "calculate perfect split for highly skewed data" in {

    // R: x <- exp(rnorm(1000))

    // Heavy right skewed data
    val g = breeze.stats.distributions.Gaussian(0, 1)
    val skewed = g.sample(1000).map(d => math.exp(d)).toArray

    val split = optimalSplit(skewed, 10, 0, 0)
    split.foreach { s =>
      s.count should be((skewed.length / 10) +- 5)
    }
    assert(split.length == 10)
    assert(split.map(_.count).sum == skewed.length)
  }


}
