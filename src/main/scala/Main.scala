import Utils.Util._
import classes.CustomPartitioner
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

//    Creazione RDD e partizione in base all'idArticolo
    val dataRDD = load_rdd(path, sc)
    val partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(4, false))

//     STAMPA DEGLI OGGETTI NELLA RELATIVA PARTIZIONE
    // it: Iterator[(String, Iterable[User])]
    partitionedRDD.mapPartitionsWithIndex(
      (index, it) => it.toList.map(println(s"PARTIZIONE:${index}", _)).iterator
    ).collect()

    val LAMBA = 10
//     HowMuch? => myHelp/Lambda*|Nodi con helpfulness minori della mia|
    println("---------------------------")
    val orderLinks = partitionedRDD.values.flatMap(users => users.map(p => (p, users.filter(
      refUser => p.helpfulness > refUser.helpfulness && p.rating == refUser.rating
    ))))
    println("-------LISTA DI ADIACENZA-------------")
    val listaAdicenza = orderLinks.map(pair => (pair._1.idUser -> (pair._2,pair._1.helpfulness)))
    listaAdicenza.foreach(println)


    println("-------Contributi (X,(value,Y)) -> X deve dare value a Y -------------")
    val test = listaAdicenza.flatMap{
      pair => {   // per ogni coppia (Donatore,(ListaDest, myHelp)
        val currentId = pair._1
        val helpfulnessCurrent = pair._2._2
        val E = pair._2._1.size
        val contrib = if (E != 0) helpfulnessCurrent / (E * LAMBA) else 0
        pair._2._1.map(u => (currentId, (u.idUser-> contrib)))
    }}

    test.foreach(println)
    println("---------------------------")
//    println("-------INIT HELPFULNESS-------------")
//    val ranksInNode = orderLinks.map(pair => (pair._1.idUser -> pair._1.helpfulness))
//    ranksInNode.foreach(println)


    //FINAL STEP:
 //test.reduceByKey(_ + _).collect().foreach(println)


    sc.stop()
  }


}
