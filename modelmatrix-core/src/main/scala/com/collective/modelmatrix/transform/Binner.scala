package com.collective.modelmatrix.transform

import breeze.linalg.DenseVector
import breeze.optimize.{DiffFunction, ApproximateGradientFunction, LBFGS}

trait Binner {

  protected case class Bin(low: Double, high: Double, count: Int)

  protected case class Quantile(percentile: Double, quantile: Double)

  protected def fromDiff(diff: Seq[Double]): Seq[Double] = {
    diff.scanLeft(0D)((acc, v) => acc + v).drop(1)
  }

  protected def toDiff(values: Seq[Double]): Seq[Double] = {

    if (values.isEmpty) {
      Seq.empty
    } else if (values.size == 1) {
      values
    } else {
      val diff = values.sliding(2) map {
        case s if s.size == 2 => s(1) - s(0)
        case s => sys.error(s"Unexpected sliding window: $s")
      }
      values.head +: diff.toSeq
    }
  }

  protected def quantiles(x: Array[Double])(percentiles: Seq[Double]): Seq[Quantile] = {
    val as = x.sorted
    percentiles.map({ p =>
      val i = p * (as.length - 1)
      val lb = i.toInt
      val ub = math.ceil(i).toInt
      val w = i - lb
      val value = as(lb) * (1 - w) + as(ub) * w
      Quantile(p, value)
    })(collection.breakOut)
  }

  /**
   * Mean squared error from ideal split
   */
  protected def error(bins: Seq[Bin], targetBins: Int): Double = {
    val sum = bins.map(_.count).sum
    bins.map(_.count - (sum / targetBins)).map(math.pow(_, 2)).sum / bins.size
  }

  protected class BinnerTargetFunction(
    x: Array[Double],
    targetBins: Int,
    minPoint: Int,
    minPct: Double
  ) extends DiffFunction[DenseVector[Double]] {

    private val min: Double = x.min
    private val max: Double = x.max

    private val minBinSize: Int =
      math.max(minPoint, x.length * minPct).toInt

    // Make penalty the same order as possible error
    private val Penalty = targetBins * math.pow(x.length / targetBins, 2)
    private val NoPenalty = 0D

    // Calculate starting point based on quantile split
    val init: DenseVector[Double] = {
      val percentile = (1 to targetBins-1) map (_.toDouble / targetBins)
      DenseVector.apply(toDiff(quantiles(x)(percentile).map(_.quantile)).toArray)
    }

    private def penalizeSmallerThenMin(p: DenseVector[Double]): Double = {
      if (p(0) < min) Penalty else NoPenalty
    }

    private def penalizeBiggerThanMax(p: DenseVector[Double]): Double = {
      if (breeze.linalg.sum(p) > max) Penalty else NoPenalty
    }

    private def penalizeNegativeDiff(p: DenseVector[Double]): Double = {
      p.activeValuesIterator.count(_ < 0) * Penalty
    }

    private def penalizeMinBinSize(p: DenseVector[Double]): Double = {
      bins(p).count(_.count < minBinSize) * Penalty
    }

    // Target minimization function
    private val targetFunction: DenseVector[Double] => Double = p => {
      error(bins(p), targetBins) +
        penalizeSmallerThenMin(p) +
        penalizeBiggerThanMax(p) +
        penalizeNegativeDiff(p) +
        penalizeMinBinSize(p)
    }

    private val gradient = new ApproximateGradientFunction(targetFunction)

    def bins(p: DenseVector[Double]): Seq[Bin] = {
      val splits = min +: fromDiff(p.toArray.filter(_ > 0)) :+ max

      val bins = splits.sliding(2) map {
        case split if split.size == 2 =>
          val low = split(0)
          val high = split(1)
          val filter = if (high < max) {
            (v: Double) => v >= low && v < high
          } else {
            (v: Double) => v >= low && v <= high
          }
          Bin(low, high, x.count(filter))

        case split => sys.error(s"Unexpected split: $split")
      }

      bins.toSeq
    }

    def calculate(p: DenseVector[Double]): (Double, DenseVector[Double]) = {
      (targetFunction(p), gradient.gradientAt(p))
    }
  }

  def optimalSplit(
    x: Array[Double],
    targetBins: Int,
    minPoints: Int = 0,
    minPct: Double = 0,
    maxIter: Int = 100,
    m: Int = 3
  ): Seq[Bin] = {

    require(targetBins >= 2, s"Target bins should greater or equal 2")

    val lbfgs = new LBFGS[DenseVector[Double]](maxIter, m)
    val f = new BinnerTargetFunction(x, targetBins, minPoints, minPct)

    val splits = lbfgs.minimize(f, f.init)
    f.bins(splits)
  }

}
