package Rank

import Custom.CustomPartitioner
import Util.User
import Util.Utils.{getResult, getTime, localHelpfulnessInit, printLinksProd}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RankProduct {
  var pathOutput = ""
  var timeout = 0
  def setPath(path : String, t: Int) ={
    pathOutput = path
    timeout = t
  }

  /* PRODUCT */
  def load_rdd(path: String, sc: SparkContext): RDD[(String, Iterable[User])] = {
    /*Utlizzate in computeProd per caricare il csv*/

    sc.textFile(path).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.map(r => {
      val fields = r.split(",")
      var top = fields(2).substring(2).toFloat
      var all = fields(3).dropRight(2).substring(1).toFloat
      val initHelpfulness = if (all == 0) 0 else localHelpfulnessInit(top, all)
      (fields(1), User(fields(5), fields(4).toInt, initHelpfulness))
    }).groupByKey()
  }

  def start(path: String, sc: SparkContext,LAMBDA: Int,ITER: Int, DEMO: Boolean, NUM_PARTITIONS: Int): Unit = {
    // Creazione RDD e partizione in base all'idArticolo

    val t_preload = System.nanoTime()
    val dataRDD = load_rdd(path, sc)
    var partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(NUM_PARTITIONS,false)).persist()
    partitionedRDD.count()
    getTime("load e part",t_preload)
    computeProd(partitionedRDD,LAMBDA,ITER,DEMO)
  }

  def computeProd(pRDD: RDD[(String, Iterable[User])], LAMBDA: Int, ITER: Int,demo: Boolean): Unit = {
    var partitionedRDD = pRDD

    if (demo) getResult(partitionedRDD,0)
    for (i <- 1 to ITER) { // INIZIO ITER
      var t0 = System.nanoTime()

      /* Struttura intermedia fatta in tal modo :
      *  (UtenteDonatore X, UtentiRiceventi,idArticolo)
      * dove UtentiRiceventi è l'insieme degli utenti con helpfulness minore di X ma che
      * hanno votato (rating) come X
      * la lista utentiRiceventi comprende anche l'user stesso il cui contributo a se stesso
      * sarà pari a 0. Serve come supporto per le fasi successive in quanto senza questo passaggio
      * l'utente potrebbe essere scartato in quanto non ha alcun contributo da nessuno
      */
      val orderLinks = partitionedRDD.flatMap {
        // idProd, user = RDD degli utenti relativi a quel prod
        case (idProd, users) => users.map(usr => (
          usr, //Utente donatore
          users.filter(refUser => (usr.helpfulness > refUser.helpfulness || (usr.idUser).eq(refUser.idUser)) && usr.rating == refUser.rating), //lista utenti riceventi
          idProd
      ))
      }


      /*-------COLLEGAMENTI IN BASE AL VOTO E ALLA HELPFUL */
      /* ogni utente "donatore" deve conoscere la sua helpfulness e la lista dei destinatari
      (X, (LISTADestinatari, Y, idArticolo) X deve dividere Y con LISTADestinatari relativo all'idArticolo
      tale struttura serve per il calcolo del contributo per ogni utente nella LISTADestinatari */
      val listaAdiacenza = orderLinks.map(pair =>
        pair._1.idUser -> (
          pair._2,
          pair._1.helpfulness,
          pair._3
        )
      )


      /* Calcolo coppie : (UserRicevente, (contributo,idArt,helpfulnessUserRicevente,RatingUserRicevente))
         il contributo verrà sommato alla helpfulnessUserRicevente nel prossimo passo */

      val contribs = listaAdiacenza.flatMap {
        pair => { // per ogni coppia (Donatore,(ListaDest, HelpfulnessDonatore, idArt))
          val (currentId,helpfulnessCurrent,idArt) = (pair._1, pair._2._2, pair._2._3)
          val E = pair._2._1.size - 1 //ogni lista contiene anche il contributo che l'user deve dare a se stesso (ossia 0)
          val contrib = if (E != 0) helpfulnessCurrent / (E * LAMBDA) else 0
          //se l'user è lo stesso allora il contributo è 0
          pair._2._1.map(userRicevente => userRicevente.idUser -> (
            if ((userRicevente.idUser).eq(currentId)) 0 else contrib, idArt, userRicevente.helpfulness, userRicevente.rating))

        }
      }

      /* (1.) Raggruppamento per articolo Y per ogni partizione in quanto ci possono essere più articoli
      * nella stessa partizione
      *  (2.) Raggruppamento per idUtente e calcolo la somma dei contributi
      *  per l'utente X relativo all'articolo Y sommando con la helpfulness
      * BISOGNA RAGRUPPARE LE CHIAVI UTILIZZANDO LE NARROW TRASFORMATIONS
      * OSSIA QUELLE TRASFORMAZIONI CHE NON CAUSANO SHUFFLING.
      * RAGGRUPPATE LE CHIAVI OSSIA FATTA LA SOMMA TOTALE DEI CONTRIBUTI
      * CHE OGNI NODO DOVRÀ AVERE SI PROCEDE CON L'UPDATE DELLE HELPFULNESS
      * ANDANDO A SOMMARE LA HELPFULNESS DI OGNI NODO CON IL RELATIVO VALORE
      * OTTENUTO DAL RAGGRUPPAMENTO
      */
      partitionedRDD = contribs.mapPartitions({ it =>
        it.toList.groupBy(_._2._2).iterator // (UserRicevente, (contributo,idArt,helpfulnessUserRicevente,RatingUserRicevente))
          .map( // 1.
            x =>
              x._1 -> //idArt, x._2 è la lista degli user
                x._2.groupBy(_._1)
                  .mapValues( // 2. raggruppamento per idUser appartenenti allo stesso articolo
                    user => { // e per ogni utente somma dei contributi e della helpfulness
                      var u = user.head //prendo il primo utente nella lista in quanto l'unico valore che varia
                    //è il contributo che verra accumulato dalla foldLeft per calcolare la nuova helpfulness
                    var userOldHelp = u._2._3 // helpfulness dell'user prima dell'update
                      val (idUser, rating) = (u._1,u._2._4)
                      val newValue = user.foldLeft(userOldHelp) { // user ha questa struttura -> (idUser,(contributo,idArticolo,helpful,rating)
                      case (acc, (_, (singleContr, _, _, _))) => { // estraggo il valore del contributo e incremento acc
                        var newHelp = acc + singleContr // che inizialmente è helpfulnessUtente
                        if (newHelp > 1.0) 1.0f
                        else if (newHelp < -1.0) -1.0f
                        else newHelp
                      }
                    }
                      /* Nuovo oggetto contenente le info necessarie per iniziare una nuova iterazione */
                      User(idUser, rating, newValue)
                    }
                  ).values //creo la lista degli oggetti utente

          )
      }, preservesPartitioning = true)


      //stampa nel terminale per il flusso in nodejs solo la prima volta per avere la topologia della rete
      if (demo && i==1) printLinksProd(listaAdiacenza)
      if(demo){
        //ad ogni fine iterazioni vengono stampati a video i rank
        getResult(partitionedRDD,i)
        Thread.sleep(timeout) //tempo al client di aggiornare la view
      }


    }/*fine ciclo*/

    /* MAPPARTITIONS NOTE:
     * https://stackoverflow.com/questions/41629953/running-groupbykey-reducebukey-on-partitioned-data-but-with-different-key*/
    var ranks = partitionedRDD.flatMap { case (idArt, users) => users.map(user => user.idUser -> user.helpfulness) }.groupByKey()
    var result = ranks.map { case (idUser, listHelpful) => {
      var size = listHelpful.size
      var sumHelpful = listHelpful.foldLeft(0f) {
        case (acc, value) => acc + value
      }
      (idUser, sumHelpful / size)
    }
    }

    /* stampa del risultato su directory output */
    var t0 = System.nanoTime()
    result.coalesce(1,shuffle = true).saveAsTextFile(pathOutput)
    println(s"----- Save RESULT (Ranks) in ${pathOutput} -----")
    getTime("Scrittura File", t0)



  }
}
