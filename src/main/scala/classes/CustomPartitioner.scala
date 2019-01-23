package classes

import org.apache.spark.Partitioner

class CustomPartitioner(override val numPartitions: Int, val debug: Boolean) extends Partitioner{
  override def getPartition(key: Any): Int = {
    val k = Math.abs(key.hashCode())
    val part = k%numPartitions
    if (debug) println(s"> ${key} in partizione ${part}")
    return k % numPartitions
  }

  override def equals(other: scala.Any): Boolean = {
    other match {
      case obj : CustomPartitioner => obj.numPartitions == numPartitions
      case _  => false
    }
  }
}
