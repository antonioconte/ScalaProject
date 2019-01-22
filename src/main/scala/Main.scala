import java.util.Scanner

import Utils.Util._   // load_rdd, printPartizione, localHelpfulness
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
    // Creazione RDD e partizione in base all'idArticolo
    val dataRDD = load_rdd(path, sc)
    /*
    * TODO: Raffinare il partizionamento in modo da avre un bilanciamente dei dati
    * CustomPartioner è una classe creata ad hoc per tale scopo
    * codice in classes.CustomerPartitioner
    * debug = true -> stampa la locazione dei dati in base all'id dell'articolo
    * */
    val partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(NUM_PARTITIONS, true))
    printPartizione(partitionedRDD)  //in Util.scala


    println("-------LISTA DI ADIACENZA-------------")
    /* è una struttura intermedia fatta in tal modo :
    (UtenteDonatore X, UtentiRiceventi)
    dove UtentiRiceventi è l'insieme degli utenti con helpfulness minore di X ma che
    hanno votato (rating) come X
    * */
    val orderLinks = partitionedRDD.values.flatMap(users => users.map(p => (p, users.filter(
      refUser => p.helpfulness > refUser.helpfulness && p.rating == refUser.rating
    ))))
    /* ogni utente "donatore" deve conoscere la sua helpfulness e la lista dei destinatari
    (X, (LISTADestinatari, Y) X deve dividere Y con LISTADestinatari
    tale struttura serve per il calcolo del contributo per ogni utente nella LISTADestinatari */
    val listaAdiacenza =orderLinks.map(pair => (pair._1.idUser -> (pair._2,pair._1.helpfulness)))
    printPartizione(listaAdiacenza) //in Util.scala

    /* Ora si calcolano le coppie (ricevente, contributo)
    ottenendo per ogni partizione la lista  di tale coppia <chiave,value>
    dove la chiave è il ricevente che deve sommare alla propria helpfulness
    un valore pari a value (contributo_ricevuto)
    * */
    println("-------Contributi (y,contributo_ricevuto) y deve ricevere un contrib pari a value-------------")
    val contribs = listaAdiacenza.flatMap{
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
    printPartizione(contribs) //in Util.scala



    /*
    * BISOGNA RAGRUPPARE LE CHIAVI UTILIZZANDO LE NARROW TRASFORMATIONS
    * OSSIA QUELLE TRASFORMAZIONI CHE NON CAUSANO SHUFFLING.
    * RAGGRUPPATE LE CHIAVI OSSIA FATTA LA SOMMA TOTALE DEI CONTRIBUTI
    * CHE OGNI NODO DOVRÀ AVERE SI PROCEDE CON L'UPDATE DELLE HELPFULNESS
    * ANDANDO A SOMMARE LA HELPFULNESS DI OGNI NODO CON IL RELATIVO VALORE
    * OTTENUTO DAL RAGGRUPPAMENTO
    *
    * > PRIMA:
    * PART0: (4,0.12)   //user 4 ha ricevuto un contr di 0.12
    * PART0: (3,0.05)   //ART-1
    * PART0: (4,0.5)    //ART-1
    * PART0: (4,0.2)    //ART-2 È DI UN ALTRO ARTICOLO PERCIÒ NON VERRA CONTATO
    * PART1: (3,0.4)    //ART-3
    * PART1: (3,0.1)    //ART-3
    * PART1: (5,0.2)    //ART-3
    *
    * > DOPO:
    * PART0: (4,0.62)   //user 4->contributo totale di 0.62 che andrà sommato alla helpfulness  ART-1
    * PART0: (3,0.05)   //ART-1
    * PART0: (4,0.2)    //user 4->contributo totale di 0.62 che andrà sommato alla helpfulness  ART-2
    * PART1: (3,0.5)    //ART-3
    * PART1: (5,0.2)    //ART-3
    *
    * ALLA FINE DELLE ITERAZIONI DOBBIAMO AVERE UNA STRUTTARE DEL GENERE:
    * PART0: (4, helpfulness-calcolata-dalle iter)    //ART-1
    * PART0: (3, helpfulness-calcolata-dalle iter)    //ART-1
    * PART0: (4, helpfulness-calcolata-dalle iter)    //ART-2
    * PART0: (3, helpfulness-calcolata-dalle iter)    //ART-3
    * PART0: (5, helpfulness-calcolata-dalle iter)    //ART-3
    *
    * reduceByKey su RDD contenente quest'ultima struttura
    * in base ad una qualche formula di riduzione (media o poi si vede)
    * */

    /*
    * Utile per non far terminare Spark e quindi accedere alla WebUI
    * http://localhost:4040/
    * se localghost=true: nella console di intellj per stoppare spark
    * inserire un carattere e premere invio
    * */
    if (localhost) new Scanner(System.in).nextLine()
    sc.stop()
  }

}
