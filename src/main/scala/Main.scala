import java.util.Scanner

import Utils.Util._
import classes.CustomPartitioner
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    disableWarning()
    val localhost = true
    val viewGraph = false //per visualizzare lo stato del grafo all'inizio di ogni iterazione
    val debug = false //ogni printPartizione causa una collect e perciò un job
    val LAMBDA = 20
    val ITER = 10
    val NUM_PARTITIONS = 4
    val path = "test.csv" //minidataset composto da una dozzina di utenti

    val conf = new SparkConf()
      .setAppName("HelpfulnessRank")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    // Creazione RDD e partizione in base all'idArticolo
    val dataRDD = load_rdd(path, sc)

    /*
    * TODO: Raffinare il partizionamento in modo da avere un bilanciamento dei dati
    * CustomPartioner è una classe creata ad hoc per tale scopo
    * codice in classes.CustomerPartitioner
    * debug = true -> stampa la locazione dei dati in base all'id dell'articolo
    * */
    var partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(NUM_PARTITIONS, debug)).persist()

    if(debug) printPartizione(partitionedRDD)  //in Util.scala

    val t0 = System.nanoTime()

    running(partitionedRDD,LAMBDA,ITER,debug,viewGraph)

    val t1 = System.nanoTime() //dopo aver partizionato

    println(s"Tempo di calcolo: (debug=${debug}, ITER=${ITER}, LAMBDA=${LAMBDA}) " + (t1 - t0)/1000000 + "ms")

    if (localhost){
    /* Utile per non far terminare Spark e quindi accedere alla WebUI
    * http://localhost:4040/
    * se localghost=true: nella console di intellj per stoppare spark
    * inserire un carattere e premere invio */
      println("> WEBUI: http://localhost:4040 -  INVIO per terminare")
      new Scanner(System.in).nextLine()
    }
    sc.stop()
  }

}
