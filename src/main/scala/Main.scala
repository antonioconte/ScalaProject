import java.util.Scanner

import Rank.{RankGeneral, RankProduct}
import Util.Utils.disableWarning
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val localhost = false

    disableWarning()
    /* LOCALE */
    //ricorda di 2.11.8 !!!!!!!!!! L O C A L E
/*
    val DEMO =false // per la stampa su file .json
    val LAMBDA = 20
    val ITER = 10
    val NUM_PARTITIONS = 3
    val path = "dataInput/cd_amazon3k.csv" //minidataset composto da una dozzina di utenti
    var typeComputation = "Product"
    var pathJson : String = "result/"
    var timeout: Int = 3000
    val conf = new SparkConf()
      .setAppName("HelpfulnessRank")
      .setMaster("local[*]")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
*/

    /*GCP */

    val DEMO = args(1).toBoolean // per la stampa su file .json
    val LAMBDA = args(4).toInt
    val ITER = args(5).toInt
    val NUM_PARTITIONS = args(6).toInt
    val path = args(2) //minidataset composto da una dozzina di utenti
    var typeComputation = args(0)
    var pathJson : String = args(3)
    var timeout: Int = args(7).toInt
    val spark = SparkSession
          .builder
          .appName("HelpfulnessRank")
          .getOrCreate()
    val sc = spark.sparkContext

    val t0 = System.nanoTime()

    typeComputation match {
      case "General" => {
        println("------ Computazione Generale -----")
        RankGeneral.setPath(pathJson,timeout)
        RankGeneral.start(path, sc, LAMBDA,DEMO,ITER,NUM_PARTITIONS)
      }
      case "Product" => {
        println("------ Computazione Per Prodotto -----")
        RankProduct.setPath(pathJson,timeout)
        RankProduct.start(path,sc,LAMBDA,ITER,DEMO,NUM_PARTITIONS)
      }
      case _ : String => {}
    }

    println(s"Tempo di calcolo: (file=${path} demo=${DEMO}, ITER=${ITER}, LAMBDA=${LAMBDA}, TIMEOUT=${timeout}) " + (System.nanoTime()-t0)/1000000000f + "s")

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
