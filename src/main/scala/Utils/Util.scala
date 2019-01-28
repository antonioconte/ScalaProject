package Utils

import classes.User
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import java.io._
case class userJson(id: String,rank: Float)
case class linkJson(source: String,target: String)

object Util {
  def localHelpfulnessInit(top: Float, all: Float) = (top - (all - top))/all

  def load_rdd(path: String, sc: SparkContext): RDD[(String,Iterable[User])] = {
    sc.textFile(path).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.map(r => {
      val fields = r.split(",")
      var top = fields(2).substring(2).toFloat
      var all = fields(3).dropRight(2).substring(1).toFloat
      val initHelpfulness = if (all == 0) 0 else localHelpfulnessInit(top,all)
      //idArt -> (idUser,Rating,Helpfulness)
      (fields(1), User(fields(5),fields(4).toInt,initHelpfulness))
    }).groupByKey()
  }


  def disableWarning(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
  }

  def printPartizione[T](value: RDD[T]): Unit ={
    //mapPartions deve tornare un iterator
    value.mapPartitionsWithIndex(
      (index, it) => it.toList.map(println(s"PARTIZIONE:${index}",_)).iterator
    ).collect()
  }
  def linkedListToJson[T](lista: RDD[(String,(Iterable[User],Float,String))]) = {
//    println("-------------JSON FILE-----------------------------")
    var list = lista.collect()
//    list.foreach(println)
    var jsonList = list.flatMap(u => {
      var idSource = u._1
      var targetList = u._2._1.filter(user => !(user.idUser).eq(u._1))
      targetList.map( u => linkJson(idSource.replace("\"",""), u.idUser.replace("\"","")))
    })
    val jsonString = write(jsonList.distinct)(DefaultFormats)
    val pw = new PrintWriter(new File("../GUI/public/links.json" ))
    pw.write(jsonString)
    pw.close()
//    println("-------------END JSON FILE-----------------------------")
  }

  def initialRankToJson(partitionedRDD: RDD[(String,Iterable[User])]) = {
    println("> STAMPA INIT IN node.json")
    var ranksToJson = partitionedRDD.flatMap{case(idArt,users) => users.map( user => user.idUser->user.helpfulness) }.groupByKey()
    var result = ranksToJson.map{case (idUser,listHelpful) => {
      var size = listHelpful.size
      var sumHelpful = listHelpful.foldLeft(0f){
        case (acc,value) => acc+value
      }
      (idUser, sumHelpful/size )
    }}
    var arrList = result.collect()
    var jsonList = arrList.map(u => userJson(u._1.replace("\"",""),u._2))
    val jsonString = write(jsonList)(DefaultFormats)
    val pw = new PrintWriter(new File("../GUI/public/nodes.json" ))
    pw.write(jsonString)
    pw.close()
    // Rank iniziale per ogni nodo: Calcolata prima dell'inizio delle iterazioni

  }




  def running[T](pRDD: RDD[(String,Iterable[User])], LAMBDA: Int, ITER: Int, debug: Boolean,viewGraph:Boolean):Unit = {
    var partitionedRDD = pRDD

    var firstTime = true
    if(debug){
      println("------ PRIMA DELLE ITERAZIONI -----")
      partitionedRDD.flatMap{case(idArt,users) => users.map( user => user.idUser->user.helpfulness) }.groupByKey().collect().foreach(println)
    }

    initialRankToJson(partitionedRDD)


    // INIZIO ITER
    for ( i <- 1 to ITER){
      Thread.sleep(5000)
      println(s"> INIZIO ITERAZIONE NUMERO -> ${i}")
      if(viewGraph)printPartizione(partitionedRDD)

      /* Struttura intermedia fatta in tal modo :
      *  (UtenteDonatore X, UtentiRiceventi,idArticolo)
      * dove UtentiRiceventi è l'insieme degli utenti con helpfulness minore di X ma che
      * hanno votato (rating) come X
      *
      * la lista utentiRiceventi comprende anche l'user stesso il cui contributo a se stesso
      * sarà pari a 0. Serve come supporto per le fasi successive
      */
      var orderLinks = partitionedRDD.flatMap{case (key,users) => users.map(p => (p, users.filter(
        refUser => (p.helpfulness > refUser.helpfulness  || p.idUser.eq(refUser.idUser) ) && p.rating == refUser.rating ), key
      ))}
      if(debug) println("-------COLLEGAMENTI IN BASE AL VOTO E ALLA HELPFUL-------------")
      if(debug) printPartizione(orderLinks) //in Util.scala
      /* ogni utente "donatore" deve conoscere la sua helpfulness e la lista dei destinatari
      (X, (LISTADestinatari, Y, idArticolo) X deve dividere Y con LISTADestinatari relativo all'idArticolo
      tale struttura serve per il calcolo del contributo per ogni utente nella LISTADestinatari */
      var listaAdiacenza = orderLinks.map(pair =>
        pair._1.idUser -> (
          pair._2,
          pair._1.helpfulness,
          pair._3
        )
      )
      if(firstTime){
        linkedListToJson(listaAdiacenza)
        firstTime = false
      }
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
      * ALLA FINE DELLE ITERAZIONI DOBBIAMO AVERE UNA STRUTTURA DEL GENERE: CALCOLARE LA HELPFULNESS LOCALE
      * PART0: (4, helpfulness-calcolata-dalle iter)    //ART-1
      * PART0: (3, helpfulness-calcolata-dalle iter)    //ART-1
      * PART0: (4, helpfulness-calcolata-dalle iter)    //ART-2
      * PART0: (3, helpfulness-calcolata-dalle iter)    //ART-3
      * PART0: (5, helpfulness-calcolata-dalle iter)    //ART-3
      */
      partitionedRDD = contribs.mapPartitions({ it =>
        it.toList.groupBy(_._2._2).iterator
          .map(   // 1.
            x =>
              x._1 ->                                      //idArt, x._2 è la lista degli user
                x._2.groupBy(_._1)
                  .mapValues(              // 2. raggruppamento per idUser appartenenti allo stesso articolo
                    user => {                                 // e per ogni utente somma dei contributi e della helpfulness
                      var u = user.head                        //prendo il primo utente nella lista in quanto l'unico valore che varia
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
                  ).values    //creo la lista degli oggetti utente

          )
      }, preservesPartitioning = true)

      getResult(partitionedRDD, debug)

    }

    /* groupByKey su RDD contenente quest'ultima struttura
    in base ad una qualche formula di riduzione (media o poi si vede)
    creazione struttura idUser -> helpfulness in modo da effettuare successivamente una groupByKey e sommare
    i risultati interemedi calcolati nei vari nodi relativi allo stesso utente */

    /*
    * >>> MAPPARTITIONS ->
    * NOTE: https://stackoverflow.com/questions/41629953/running-groupbykey-reducebukey-on-partitioned-data-but-with-different-key
    * RDD.mapPartitions({ it =>
    *       it.toList.groupBy(_._1).mapValues(_.size) // some grouping + reducing the result
    *       .iterator
    * }, preservesPartitioning = true)
    * */
    if(debug) println("---------------------")

  }


  //last = false allora prendo il risultato intermedio
  def getResult(partitionedRDD: RDD[(String,Iterable[User])], debug: Boolean) = {
    var ranks = partitionedRDD.flatMap{case(idArt,users) => users.map( user => user.idUser->user.helpfulness) }.groupByKey()
    if(debug){
      println("------ALLA FINE DELLE ITER---------")
      ranks.collect().foreach(println) //prima della somma
    }
    println("------RESULT---------")
    var result = ranks.map{case (idUser,listHelpful) => {
      var size = listHelpful.size
      var sumHelpful = listHelpful.foldLeft(0f){
        case (acc,value) => acc+value
      }
      (idUser, sumHelpful/size )
    }}
    var arrList = result.collect()
    var jsonList = arrList.map(u => userJson(u._1.replace("\"",""),u._2))
    val jsonString = write(jsonList)(DefaultFormats)
    val pw = new PrintWriter(new File("../GUI/public/nodes.json" ))
    pw.write(jsonString)
    pw.close()
    //    result.collect().foreach(println)
  }
}
