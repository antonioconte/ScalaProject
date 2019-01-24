package Utils

import classes.User
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
      (fields(1), new User(fields(5),fields(4).toInt,initHelpfulness))

    }).groupByKey()
  }


  def disableWarning(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
  }

  def printPartizione[T](value: RDD[T]): Unit ={
    //mapPartions deve tornare un iterator
    value.mapPartitionsWithIndex(
      (index, it) => it.toList.map(p => println(s"PARTIZIONE:${index}", p )).iterator
    ).collect()
  }


  def running[T](pRDD : RDD[(String,Iterable[User])], LAMBDA: Int, debug: Boolean):Unit = {
    var partitionedRDD = pRDD
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

  }

}
