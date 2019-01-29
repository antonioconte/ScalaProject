package classes

import org.apache.spark.Partitioner

import scala.collection.Map

class CustomPartitioner(override val numPartitions: Int, val debug: Boolean) extends Partitioner{
  var hashMap = Array.fill(numPartitions){0}
  var count = Map()
  var rddMapCount : Map[String,Int] = null

  def this(numPartitions: Int,debug: Boolean,rdd: Map[String, Int]) = {
    this(numPartitions,debug)
    rddMapCount = rdd
  }

  override def getPartition(key: Any): Int = {
    var numElem = rddMapCount.get(key.toString).get
    var minIndex = hashMap.zipWithIndex.min._2
    var minValue = hashMap(minIndex)
    hashMap(minIndex) = minValue + numElem
    println(s"> ${key} -> ${numElem} Partizione [${minIndex}]")
    return minIndex

    //    val k = Math.abs(key.hashCode())
//    val part = k%numPartitions
//    if (debug) println(s"> ${key} in partizione ${part}")
//    return k % numPartitions
  }

  override def equals(other: scala.Any): Boolean = {
    other match {
      case obj : CustomPartitioner => obj.numPartitions == numPartitions
      case _  => false
    }
  }

  def getHashMap(): Unit ={
    hashMap.indices.foreach(i => println(s"> Part: ${i} -> ${hashMap(i)}"))
  }
}
