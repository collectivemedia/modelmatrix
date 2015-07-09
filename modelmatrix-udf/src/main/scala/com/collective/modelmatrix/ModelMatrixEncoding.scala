package com.collective.modelmatrix

import java.nio.ByteBuffer

import scodec.bits.ByteVector

/**
 * Model Matrix stores original feature value as Array[Byte] and uses
 * scodec.bites.ByteVector for encoding/decoding
 */
object ModelMatrixEncoding {

  trait Encode[T] {
    def encode(obj: T): ByteVector
  }

  object Encode {
    def apply[T](f: T => ByteVector): Encode[T] = new Encode[T] {
      def encode(obj: T): ByteVector = f(obj)
    }

    implicit val EncodeShort: Encode[Short] =
      Encode((short: Short) => ByteVector(ByteBuffer.allocate(2).putShort(short).array()))
    implicit val EncodeInt: Encode[Int] =
      Encode((integer: Int) => ByteVector(ByteBuffer.allocate(4).putInt(integer).array()))
    implicit val EncodeLong: Encode[Long] =
      Encode((long: Long) => ByteVector(ByteBuffer.allocate(8).putLong(long).array()))
    implicit val EncodeDouble: Encode[Double] =
      Encode((double: Double) => ByteVector(ByteBuffer.allocate(8).putDouble(double).array()))
    implicit val EncodeString: Encode[String] =
      Encode((string: String) => ByteVector(string.getBytes))

  }

  trait Decode[T] {
    def decode(bytes: ByteVector): T
  }

  object Decode {
    def apply[T](f: ByteVector => T): Decode[T] = new Decode[T] {
      def decode(bytes: ByteVector): T = f(bytes)
    }

    implicit val DecodeShort: Decode[Short] =
      Decode(_.toByteBuffer.getShort)
    implicit val DecodeInt: Decode[Int] =
      Decode(_.toByteBuffer.getInt)
    implicit val DecodeLong: Decode[Long] =
      Decode(_.toByteBuffer.getLong)
    implicit val DecodeDouble: Decode[Double] =
      Decode(_.toByteBuffer.getDouble)
    implicit val DecodeString: Decode[String] =
      Decode(b => new String(b.toArray))

  }

  def encode[T: Encode](obj: T): ByteVector = implicitly[Encode[T]].encode(obj)

  def decode[T: Decode](obj: ByteVector): T = implicitly[Decode[T]].decode(obj)

}
