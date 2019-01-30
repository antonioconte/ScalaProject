import java.util.Scanner

import Utils.Util._
import org.apache.spark.{SparkConf, SparkContext}


object Main {

  def main(args: Array[String]): Unit = {

    disableWarning()
    val localhost = false
    val DEBUG = false //ogni printPartizione causa una collect e perciò un job
    val DEMO = true // per la stampa su file .json
    val LAMBDA = 20
    val ITER = 10
    val NUM_PARTITIONS = 4
    val path = "demo.csv" //minidataset composto da una dozzina di utenti
//    val path = "3prod10users24review.csv"
//    val path = "cd_amazon.csv"
//    val path = "test.csv"

    val typeComputation = "General"

    val conf = new SparkConf()
      .setAppName("HelpfulnessRank")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)


    val t0 = System.nanoTime()
    typeComputation match {
      case "General" => {
        println("------ Computazione Generale -----")
        startComputeGeneral(path, sc, LAMBDA,DEMO,ITER,DEBUG,NUM_PARTITIONS)
      }
      case "Product" => {
        println("------ Computazione Per Prodotto -----")
        startComputeProd(path,sc,LAMBDA,ITER,DEMO,DEBUG,NUM_PARTITIONS)
      }
      case _ : String => {}
    }


    val t1 = System.nanoTime() //dopo aver partizionato

    println(s"Tempo di calcolo: (debug=${DEBUG},demo=${DEMO}, ITER=${ITER}, LAMBDA=${LAMBDA}) " + (t1 - t0) / 1000000 + "ms")

    if (localhost) {
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
