package classes

import org.apache.spark.Partitioner
import scala.collection.Map

class CustomizedPartitioner(override val numPartitions: Int, val debug: Boolean, val rdd: Map[String, Iterable[UserComment]], var partitionSize: Array[Int]) extends Partitioner {

  //  def partitionSize = Array.fill(numPartitions)(0)

  override def getPartition(key: Any): Int = {

    val numComm = rdd.get(key.toString).get.toList.length
    println(key , numComm)
    var min = partitionSize(0)
    var indMin = 0
    println(indMin, partitionSize(0))


    for(i <- 1 until numPartitions){
      println(i, partitionSize(i))
      if (partitionSize(i)<min){
        min=partitionSize(i)
        indMin=i
      }
    }
    println()

    println(indMin, min)

    println(min)
    println(indMin)


    partitionSize(indMin) = partitionSize(indMin) + numComm
    //    print(partitionSize(indMin))

    return indMin

    //    val k = Math.abs(key.hashCode())
    //    val part = k%numPartitions
    //    if (debug) println(s"> ${key} in partizione ${part}")
    //    return k % numPartitions
  }

  /*
  override def equals(other: scala.Any): Boolean = {
    other match {
      case obj : CustomPartitioner => obj.numPartitions == numPartitions
      case _  => false
    }
  }
 */

}