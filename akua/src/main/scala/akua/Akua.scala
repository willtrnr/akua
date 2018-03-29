package akua

object Akua extends
  serializer.AllSerializers with
  ToDistinctOps with
  ToExistsOps with
  ToHashJoinOps with
  ToNotExistsOps with
  ToSpillingHashJoinOps with
  ToSpillingDistinctOps
