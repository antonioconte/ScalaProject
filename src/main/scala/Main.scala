import classes.{CustomPartitioner, User}
import Utils.Util._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    disableWarning()
    val path = "prod15users.csv"
    val conf = new SparkConf()
      .setAppName("HelpfulnessRank")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    /*  0 : numero riga (non interessa)
        1 : idArticolo
        2 : helpful
        3 : overall
        4 : idUser
        5 : unixTime
    */

    //Creazione RDD e partizione in base all'idArticolo
    val dataRDD = load_rdd(path, sc)
    val partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(4, false))
    // STAMPA DEGLI OGGETTI NELLA RELATIVA PARTIZIONE
    partitionedRDD.mapPartitionsWithIndex(
      (index: Int, it: Iterator[(String, Iterable[User])]) => it.toList.map(println(s"PARTIZIONE:${index}", _)).iterator
    ).collect()
    
    // HowMuch? => myHelp/Lambda*|Nodi con helpfulness minori della mia|
    println("---------------------------")
    val orderLinks = partitionedRDD.values.flatMap(users => users.map(p => (p, users.filter(
      refUser => p.helpfulness > refUser.helpfulness && p.rating == refUser.rating
    ))))

    val listaAdicenza = orderLinks.map(pair => (pair._1.idUser, pair._2.map(p => p.idUser)))
    val ranksInNode = orderLinks.map(pair => (pair._1.idUser, pair._1.helpfulness))

    listaAdicenza.foreach(println)

    // ranksInNode.foreach(println)
    sc.stop()
  }


  def disableWarning(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
  }
}
