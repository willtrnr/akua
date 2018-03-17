package akua

import org.mapdb.{DataIO, DataInput2, DataOutput2, Serializer}

trait StdSerializers {

  implicit val boolSerializer: Serializer[Boolean] = Serializer.BOOLEAN.asInstanceOf[Serializer[Boolean]]
  implicit val byteSerializer: Serializer[Byte] = Serializer.BYTE.asInstanceOf[Serializer[Byte]]
  implicit val charSerializer: Serializer[Char] = Serializer.CHAR.asInstanceOf[Serializer[Char]]
  implicit val shortSerializer: Serializer[Short] = Serializer.SHORT.asInstanceOf[Serializer[Short]]
  implicit val intSerializer: Serializer[Int] = Serializer.INTEGER.asInstanceOf[Serializer[Int]]
  implicit val longSerializer: Serializer[Long] = Serializer.LONG.asInstanceOf[Serializer[Long]]
  implicit val floatSerializer: Serializer[Float] = Serializer.FLOAT.asInstanceOf[Serializer[Float]]
  implicit val doubleSerializer: Serializer[Double] = Serializer.DOUBLE.asInstanceOf[Serializer[Double]]

  implicit val stringSerializer: Serializer[String] = Serializer.STRING

  implicit val jBigIntegerSerializer: Serializer[java.math.BigInteger] = Serializer.BIG_INTEGER
  implicit val jBigDecimalSerializer: Serializer[java.math.BigDecimal] = Serializer.BIG_DECIMAL

  implicit val uuidSerializer: Serializer[java.util.UUID] = Serializer.UUID

  implicit val byteArraySerializer: Serializer[Array[Byte]] = Serializer.BYTE_ARRAY
  implicit val charArraySerializer: Serializer[Array[Char]] = Serializer.CHAR_ARRAY
  implicit val shortArraySerializer: Serializer[Array[Short]] = Serializer.SHORT_ARRAY
  implicit val intArraySerializer: Serializer[Array[Int]] = Serializer.INT_ARRAY
  implicit val longArraySerializer: Serializer[Array[Long]] = Serializer.LONG_ARRAY
  implicit val floatArraySerializer: Serializer[Array[Float]] = Serializer.FLOAT_ARRAY
  implicit val doubleArraySerializer: Serializer[Array[Double]] = Serializer.DOUBLE_ARRAY

  implicit def optionSerializer[A](implicit s: Serializer[A]): Serializer[Option[A]] = new Serializer[Option[A]] {

    override def serialize(out: DataOutput2, value: Option[A]): Unit = {
      out.writeBoolean(value.isDefined)
      for (v <- value) s.serialize(out, v)
    }

    override def deserialize(input: DataInput2, available: Int): Option[A] =
      if (input.readBoolean()) Some(s.deserialize(input, available - 1)) else None

    override val fixedSize: Int =
      if (s.fixedSize >= 0) s.fixedSize + 1 else -1

    override val isTrusted: Boolean =
      s.isTrusted

    override def hashCode(o: Option[A], seed: Int): Int =
      o.fold(seed)(v => seed + DataIO.intHash(s.hashCode(v, seed)))

    override def clone(value: Option[A]): Option[A] =
      value.map(s.clone)

  }

  implicit def vectorSerializer[A](implicit s: Serializer[A]): Serializer[Vector[A]] = new Serializer[Vector[A]] {

    override def serialize(out: DataOutput2, value: Vector[A]): Unit = {
      out.packInt(value.size)
      for (v <- value) s.serialize(out, v)
    }

    override def deserialize(input: DataInput2, available: Int): Vector[A] =
      Vector.fill(input.unpackInt())(s.deserialize(input, -1))

    override def isTrusted: Boolean =
      s.isTrusted

    override def hashCode(o: Vector[A], seed: Int): Int = {
      var ss = seed
      for (v <- o) ss += DataIO.intHash(s.hashCode(v, ss))
      ss
    }

    override def clone(value: Vector[A]): Vector[A] =
      value.map(s.clone)

  }

}

object StdSerializers extends StdSerializers
