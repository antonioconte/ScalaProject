import java.util.Scanner

import Utils.Util._
import classes.{CustomPartitioner, User}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    disableWarning()
    val localhost = false
    val debug = true //ogni printPartizione causa una collect e perciò un job
    val LAMBDA = 10
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
    var partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(NUM_PARTITIONS, debug))

    if(debug) printPartizione(partitionedRDD)  //in Util.scala
    /* è una struttura intermedia fatta in tal modo :
    (UtenteDonatore X, UtentiRiceventi)
    dove UtentiRiceventi è l'insieme degli utenti con helpfulness minore di X ma che
    hanno votato (rating) come X
    * */
    val t0 = System.nanoTime() //dopo aver partizionato

    var orderLinks = partitionedRDD.flatMap{case (key,users) => users.map(p => (p, users.filter(
      refUser => p.helpfulness >= refUser.helpfulness && p.rating == refUser.rating ), key
    ))}
    if(debug) println("-------COLLEGAMENTI IN BASE AL VOTO E ALLA HELPFUL-------------")

    if(debug) printPartizione(orderLinks) //in Util.scala

    /* ogni utente "donatore" deve conoscere la sua helpfulness e la lista dei destinatari
    (X, (LISTADestinatari, Y, idArticolo) X deve dividere Y con LISTADestinatari relativo all'idArticolo
    tale struttura serve per il calcolo del contributo per ogni utente nella LISTADestinatari */
    var listaAdiacenza =orderLinks.map(pair => (pair._1.idUser -> (pair._2,
      pair._1.helpfulness,
      pair._3)))
    if(debug) println("-------LISTA DI ADIACENZA-------------")
    if(debug) printPartizione(listaAdiacenza) //in Util.scala

    /* Ora si calcolano le coppie (ricevente, contributo)
    ottenendo per ogni partizione la lista  di tale coppia <chiave,value>
    dove la chiave è il ricevente che deve sommare alla propria helpfulness
    un valore pari a value (contributo_ricevuto)
    * */
    if(debug) println("----Contributi (y,(value, idArt, helpfull di y)) y deve ricevere un contrib pari a value per aver commentato lo stesso idArt -------------")
    val contribs = listaAdiacenza.flatMap{
      pair => {   // per ogni coppia (Donatore,(ListaDest, myHelp)
        val currentId = pair._1
        val helpfulnessCurrent = pair._2._2
        val E = pair._2._1.size-1   //ogni lista contiene anche il contributo che l'user deve dare a se stesso (ossia 0)
                                    //serve per ottenere la lista completa delle helpfulness
        val idArt  = pair._2._3
        //     HowMuch? => myHelp/Lambda*|Nodi con helpfulness minori della mia|
        val contrib = if (E != 0) helpfulnessCurrent / (E * LAMBDA) else 0
        //se l'user è lo stesso allora il contributo è 0
        pair._2._1.map(userRicevente => userRicevente.idUser-> (
          if( (userRicevente.idUser).eq(currentId) ) 0 else contrib,idArt,userRicevente.helpfulness,userRicevente.rating))

    }}
    if(debug) printPartizione(contribs) //in Util.scala

    /* (1.) Raggruppamento per articolo Y per ogni partizione in quanto ci possono essere più articoli
    * nella stessa partizione
    *  (2.) Raggruppamento per idUtente e calcolo la somma dei contributi
    *  per l'utente X relativo all'articolo Y sommando con la helpfulness */
    if(debug) println("--------SOMMA CONTRIBS E Helpfulness PER USER (stesso articolo)------------")
    partitionedRDD = contribs.mapPartitions({ it =>
      it.toList.groupBy(_._2._2).iterator
        .map(   // 1.
        x =>
          x._1 ->                                      //idArt, x._2 è la lista degli user
          x._2.groupBy(_._1)
            .mapValues(              // 2. raggruppamento per idUser appartenenti allo stesso articolo
           user => {                                 // e per ogni utente somma dei contributi e della helpfulness
             var u = user(0)                        //prendo il primo utente nella lista in quanto l'unico valore che varia
                                                    //è il contributo che verra accumulato dalla foldLeft per calcolare la nuova helpfulness
             var userOldHelp = u._2._3              // helpfulness dell'user prima dell'update
             var newValue = user.foldLeft(userOldHelp) {                      // user ha questa struttura -> (idUser,(contributo,idArticolo,helpful,rating)
               case (acc, (idUser,(singleContr,idArt,help,rating))) => {          // estraggo il valore del contributo e incremento acc
                 var newHelp = acc + singleContr               // che inizialmente è helpfulnessUtente
                 if (newHelp > 1.0) 1.0f
                 else if (newHelp < -1.0) -1.0f
                 else newHelp
               }
             }
             new User(u._1,u._2._4,newValue)        //creo nuovo oggetto contenente le info necessarie per iniziare una nuova iterazione
           }
          ).map(x => x._2)   //creo la lista degli oggetti utente

      )
    }, preservesPartitioning = true)

    printPartizione(partitionedRDD)
    if(debug) println("---------------------")
    val t1 = System.nanoTime() //dopo aver partizionato
    println(s"Tempo di calcolo (dopo il partizionamento): (debug=${debug}) " + (t1 - t0)/1000000 + "ms")


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
    * >>>>>> FATTO : aggrContrs permette questo
    * TODO:>>> ALLA FINE DELLE ITERAZIONI DOBBIAMO AVERE UNA STRUTTURA DEL GENERE: CALCOLARE LA HELPFULNESS LOCALE
    * PART0: (4, helpfulness-calcolata-dalle iter)    //ART-1
    * PART0: (3, helpfulness-calcolata-dalle iter)    //ART-1
    * PART0: (4, helpfulness-calcolata-dalle iter)    //ART-2
    * PART0: (3, helpfulness-calcolata-dalle iter)    //ART-3
    * PART0: (5, helpfulness-calcolata-dalle iter)    //ART-3

    * reduceByKey su RDD contenente quest'ultima struttura
    * in base ad una qualche formula di riduzione (media o poi si vede)
    * */

    /*
    * Utile per non far terminare Spark e quindi accedere alla WebUI
    * http://localhost:4040/
    * se localghost=true: nella console di intellj per stoppare spark
    * inserire un carattere e premere invio
    *
    * NOTE: https://stackoverflow.com/questions/41629953/running-groupbykey-reducebukey-on-partitioned-data-but-with-different-key
    * RDD.mapPartitions({ it =>
    *       it.toList.groupBy(_._1).mapValues(_.size) // some grouping + reducing the result
    *       .iterator
    * }, preservesPartitioning = true)
    * */
    if (localhost){
      println("> WEBUI: http://localhost:4040 -  INVIO per terminare")
      new Scanner(System.in).nextLine()
    }
    sc.stop()
  }

}
