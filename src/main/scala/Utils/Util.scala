package Utils

import classes.User
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Util {
  def localHelpfulnessInit(top: Float, all: Float) = (top - (all - top))/all

  def load_rdd(path: String, sc: SparkContext): RDD[(String,Iterable[User])] = {
    sc.textFile(path).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.map(r => {


      val fields = r.split(",")
      var top = fields(2).substring(2).toFloat
      var all = fields(3).dropRight(2).substring(1).toFloat
      val initHelpfulness = if (all == 0) 0 else localHelpfulnessInit(top,all)
      //idArt -> (idUser,Rating,Helpfulness)
      (fields(1), new User(fields(5),fields(4).toInt,initHelpfulness))

    }).groupByKey().persist()
  }


  def disableWarning(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
  }

  def printPartizione[T](value: RDD[T]): Unit ={
    //mapPartions deve tornare un iterator
    value.mapPartitionsWithIndex(
      (index, it) => it.toList.map(p => println(s"PARTIZIONE:${index}", p )).iterator
    ).collect()
  }

}
