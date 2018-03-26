package akua.serializer

import org.mapdb.{DataInput2, DataOutput2, Serializer}

final class SerializerOps[A](self: Serializer[A]) {

  def xmap[B](from: A => B, to: B => A): Serializer[B] = new Serializer[B] {

    override def serialize(out: DataOutput2, value: B): Unit = {
      self.serialize(out, to(value))
    }

    override def deserialize(input: DataInput2, available: Int): B =
      from(self.deserialize(input, available))

    override val fixedSize: Int =
      self.fixedSize

    override val isTrusted: Boolean =
      self.isTrusted

    override def hashCode(o: B, seed: Int): Int =
      self.hashCode(to(o), seed)

    override def clone(value: B): B =
      from(self.clone(to(value)))

  }

}

trait ToSerializerOps {
  implicit def toSerializerOps[A](s: Serializer[A]): SerializerOps[A] = new SerializerOps(s)
}
