package akua

import shapeless.{Generic, HList, HNil, ::}

import org.mapdb.{DataIO, DataInput2, DataOutput2, Serializer}

trait GenericSerializers {

  implicit val hnilSerializer: Serializer[HNil] = new Serializer[HNil] {
    override def serialize(out: DataOutput2, value: HNil): Unit = ()
    override def deserialize(input: DataInput2, available: Int): HNil = HNil
    override val fixedSize: Int = 0
    override val isTrusted: Boolean = true
    override def hashCode(o: HNil, seed: Int): Int = seed
    override def clone(value: HNil): HNil = value
  }

  implicit def hlistSerializer[H, T <: HList](implicit hs: Serializer[H], ts: Serializer[T]): Serializer[H :: T] = new Serializer[H :: T] {

    override def serialize(out: DataOutput2, value: H :: T): Unit = {
      hs.serialize(out, value.head)
      ts.serialize(out, value.tail)
    }

    override def deserialize(input: DataInput2, available: Int): H :: T =
      hs.deserialize(input, -1) :: ts.deserialize(input, -1)

    override val fixedSize: Int =
      if (hs.fixedSize >= 0 && ts.fixedSize >= 0) hs.fixedSize + ts.fixedSize else -1

    override val isTrusted: Boolean =
      hs.isTrusted && ts.isTrusted

    override def hashCode(o: H :: T, seed: Int): Int = {
      var s = seed
      s += DataIO.intHash(hs.hashCode(o.head, s))
      s += DataIO.intHash(ts.hashCode(o.tail, s))
      s
    }

    override def clone(value: H :: T): H :: T =
      value

  }

  implicit def genericSerializer[A, R](implicit gen: Generic[A] { type Repr = R }, ser: Serializer[R]): Serializer[A] = new Serializer[A] {
    override def serialize(out: DataOutput2, value: A): Unit = ser.serialize(out, gen.to(value))
    override def deserialize(input: DataInput2, available: Int): A = gen.from(ser.deserialize(input, available))
    override val fixedSize: Int = ser.fixedSize
    override val isTrusted: Boolean = ser.isTrusted
    override def hashCode(o: A, seed: Int): Int = ser.hashCode(gen.to(o), seed)
    override def clone(value: A): A = gen.from(ser.clone(gen.to(value)))
  }

}

object GenericSerializers extends GenericSerializers
