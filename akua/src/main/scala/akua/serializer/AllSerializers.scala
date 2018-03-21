package akua.serializer

trait AllSerializers extends
  StdSerializers with
  TupleSerializers with
  GenericSerializers

object AllSerializers extends AllSerializers
