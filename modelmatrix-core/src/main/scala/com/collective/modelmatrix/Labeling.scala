package com.collective.modelmatrix

import scala.reflect.ClassTag


sealed trait Labeling[L] extends Serializable {

  def expressions: Seq[String]

  def label(values: Seq[Any]): L

}

object Labeling {

  // scalastyle:off

  val nil: Labeling[Unit] = new Labeling[Unit] {
    val expressions: Seq[String] = Nil
    def label(values: Seq[Any]): Unit = sys.error(s"No labeling is supported")
  }

  def apply[L, T1: ClassTag](exp: String, f: T1 => L): Labeling[L] =
    new UserDefinedLabeling1[L, T1](exp, f)

  def apply[L, T1, T2](exp1: String, exp2: String, f: (T1, T2) => L): Labeling[L] =
    new UserDefinedLabeling2[L, T1, T2](exp1, exp2, f)

  def apply[L, T1, T2, T3](exp1: String, exp2: String, exp3: String, f: (T1, T2, T3) => L): Labeling[L] =
    new UserDefinedLabeling3[L, T1, T2, T3](exp1, exp2, exp3, f)

  def apply[L, T1, T2, T3, T4](exp1: String, exp2: String, exp3: String, exp4: String, f: (T1, T2, T3, T4) => L): Labeling[L] =
    new UserDefinedLabeling4[L, T1, T2, T3, T4](exp1, exp2, exp3, exp4, f)

  def apply[L, T1, T2, T3, T4, T5](exp1: String, exp2: String, exp3: String, exp4: String, exp5: String, f: (T1, T2, T3, T4, T5) => L): Labeling[L] =
    new UserDefinedLabeling5[L, T1, T2, T3, T4, T5](exp1, exp2, exp3, exp4, exp5, f)

  private class UserDefinedLabeling1[L, T1: ClassTag](exp: String, f: T1 => L) extends Labeling[L] {
    val expressions: Seq[String] = Seq(exp)

    def label(values: Seq[Any]): L = {
      require(values.length == 1, s"Wrong number of evaluated columns: ${values.length}. Expected: 1")
      f(values.head.asInstanceOf[T1])
    }
  }

  private class UserDefinedLabeling2[L, T1, T2](exp1: String, exp2: String, f: (T1, T2) => L) extends Labeling[L] {
    val expressions: Seq[String] = Seq(exp1, exp2)

    def label(values: Seq[Any]): L = {
      require(values.length == 2, s"Wrong number of evaluated Strings: ${values.length}. Expected: 2")
      f(values(0).asInstanceOf[T1], values(1).asInstanceOf[T2])
    }
  }

  private class UserDefinedLabeling3[L, T1, T2, T3](exp1: String, exp2: String, exp3: String, f: (T1, T2, T3) => L) extends Labeling[L] {
    val expressions: Seq[String] = Seq(exp1, exp2, exp3)

    def label(values: Seq[Any]): L = {
      require(values.length == 3, s"Wrong number of evaluated Strings: ${values.length}. Expected: 3")
      f(values(0).asInstanceOf[T1], values(1).asInstanceOf[T2], values(2).asInstanceOf[T3])
    }
  }

  private class UserDefinedLabeling4[L, T1, T2, T3, T4](exp1: String, exp2: String, exp3: String, exp4: String, f: (T1, T2, T3, T4) => L) extends Labeling[L] {
    val expressions: Seq[String] = Seq(exp1, exp2, exp3, exp4)

    def label(values: Seq[Any]): L = {
      require(values.length == 3, s"Wrong number of evaluated Strings: ${values.length}. Expected: 3")
      f(values(0).asInstanceOf[T1], values(1).asInstanceOf[T2], values(2).asInstanceOf[T3], values(3).asInstanceOf[T4])
    }
  }

  private class UserDefinedLabeling5[L, T1, T2, T3, T4, T5](exp1: String, exp2: String, exp3: String, exp4: String, exp5: String, f: (T1, T2, T3, T4, T5) => L) extends Labeling[L] {
    val expressions: Seq[String] = Seq(exp1, exp2, exp3, exp4, exp5)

    def label(values: Seq[Any]): L = {
      require(values.length == 3, s"Wrong number of evaluated Strings: ${values.length}. Expected: 3")
      f(values(0).asInstanceOf[T1], values(1).asInstanceOf[T2], values(2).asInstanceOf[T3], values(3).asInstanceOf[T4], values(4).asInstanceOf[T5])
    }
  }

  // scalastyle:on

}

