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
      val fields = r.split(";")
      var top_all = fields(2).toString.split(",")
      var top = top_all(0).substring(1).toFloat
      var all = top_all(1).dropRight(1).substring(1).toFloat
      val initHelpfulness = if (all == 0) 0 else localHelpfulnessInit(top,all)
      (fields(1), new User(fields(4),fields(3).toInt,initHelpfulness))
    }).groupByKey().persist()
  }


  def disableWarning(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
  }
}
