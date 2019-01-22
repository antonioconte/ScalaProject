import java.util.Scanner

import Utils.Util._
import classes.CustomPartitioner
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    disableWarning()
    val localhost = false
    val LAMBA = 10
    val NUM_PARTITIONS = 4
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

//    Creazione RDD e partizione in base all'idArticolo
    val dataRDD = load_rdd(path, sc)
    val partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(NUM_PARTITIONS, true))
//     STAMPA DEGLI OGGETTI NELLA RELATIVA PARTIZIONE ... it: Iterator[(String, Iterable[User])]
    partitionedRDD.mapPartitionsWithIndex(
      (index, it) => it.toList.map(p => println(s"PARTIZIONE:${index}", p._1,p._2.toList )).iterator
    ).collect()


    val orderLinks = partitionedRDD.values.flatMap(users => users.map(p => (p, users.filter(
      refUser => p.helpfulness > refUser.helpfulness && p.rating == refUser.rating
    ))))
    println("-------LISTA DI ADIACENZA-------------")
    val listaAdicenza = orderLinks.map(pair => (pair._1.idUser -> (pair._2,pair._1.helpfulness)))
    listaAdicenza.mapPartitionsWithIndex(
      (index, it) => it.toList.map(p => println(s"PARTIZIONE:${index}", p )).iterator
    ).collect()

    println("-------Contributi (y,value) y deve ricevere un contrib pari a value-------------")
    //flatMap
    val contribs = listaAdicenza.flatMap{
      pair => {   // per ogni coppia (Donatore,(ListaDest, myHelp)
        val currentId = pair._1
        val helpfulnessCurrent = pair._2._2
        val E = pair._2._1.size
        //     HowMuch? => myHelp/Lambda*|Nodi con helpfulness minori della mia|
        val contrib = if (E != 0) helpfulnessCurrent / (E * LAMBA) else 0
        pair._2._1.map(u => (currentId, u.idUser-> contrib))
    }}.mapPartitions(
      part => part.map( p => (p._2._1 -> p._2._2)),true

    )
    contribs.mapPartitionsWithIndex(
      (index, it) => it.toList.map(p => println(s"PARTIZIONE:${index}", p )).iterator
    ).collect()
    println("-------------------")

//    contribs.mapPartitions( p => {
//        p.map(_._1)
//      }).mapPartitionsWithIndex(
//      (index, it) => it.toList.map(p => println(s"PARTIZIONE:${index}", p )).iterator
//    ).collect().foreach(println)

    //test.reduceByKey(_ + _).collect().foreach(println)
    if (localhost) new Scanner(System.in).nextLine()
    sc.stop()
  }


}
