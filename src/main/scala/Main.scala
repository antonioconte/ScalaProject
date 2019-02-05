import java.util.Scanner

import Utils.Util
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object Main {

  def main(args: Array[String]): Unit = {

    Util.disableWarning()
    val localhost = false
    val DEBUG = false //ogni printPartizione causa una collect e perciò un job
    val DEMO = args(1).toBoolean // per la stampa su file .json
    val LAMBDA = args(4).toInt
    val ITER = args(5).toInt
    val NUM_PARTITIONS = args(6).toInt
    val path = args(2) //minidataset composto da una dozzina di utenti
    var typeComputation = args(0)
    var pathJson : String = args(3)
    var timeout: Int = args(7).toInt
    Util.setPath(pathJson,timeout)

    //    val conf = new SparkConf()
    //      .setAppName("HelpfulnessRank")
    //      .setMaster("local[*]")
    //      .set("spark.hadoop.validateOutputSpecs", "false")

    val spark = SparkSession
      .builder
      .appName("HelpfulnessRank")
      .getOrCreate()

    val sc = spark.sparkContext


    //    val sc = new SparkContext(conf)

    val t0 = System.nanoTime()
    typeComputation match {
      case "General" => {
        println("------ Computazione Generale -----")
        Util.startComputeGeneral(path, sc, LAMBDA,DEMO,ITER,DEBUG,NUM_PARTITIONS)
      }
      case "Product" => {
        println("------ Computazione Per Prodotto -----")
        Util.startComputeProd(path,sc,LAMBDA,ITER,DEMO,DEBUG,NUM_PARTITIONS)
      }
      case _ : String => {}
    }


    val t1 = System.nanoTime() //dopo aver partizionato

    println(s"Tempo di calcolo: (debug=${DEBUG},demo=${DEMO}, ITER=${ITER}, LAMBDA=${LAMBDA}, TIMEOUT=${timeout}) " + (t1 - t0) / 1000000 + "ms")

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
