package Utils
import java.text.SimpleDateFormat
import java.util.Date

import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

/*CLASSI AUSILIARIE PER LA CREAZIONE DEL JSON*/
case class userJson(id: String, rank: Float)
case class linkJson(source: String, target: String)

case class User(idUser:String,rating:Int, helpfulness:Float)
case class UserComment(idProd:String,rating:Int,helpfulness:Float)

class CustomPartitioner(override val numPartitions: Int, val debug: Boolean) extends Partitioner{

  override def getPartition(key: Any): Int = {
    //    var numElem = rddMapCount.get(key.toString).get
    //    var minIndex = hashMap.zipWithIndex.min._2
    //    var minValue = hashMap(minIndex)
    //    hashMap(minIndex) = minValue + numElem
    //    println(s"> ${key} -> ${numElem} Partizione [${minIndex}]")
    //    return minIndex

    val k = Math.abs(key.hashCode())
    val part = k%numPartitions
    if (debug) println(s"> ${key} in partizione ${part}")
    return k % numPartitions
  }

  override def equals(other: scala.Any): Boolean = {
    other match {
      case obj : CustomPartitioner => obj.numPartitions == numPartitions
      case _  => false
    }
  }

}

object Util {

  var pathOutput = ""

  var timeout = 0
  def setPath(path : String, t: Int) ={
    pathOutput = path
    timeout = t
  }

  def localHelpfulnessInit(top: Float, all: Float) = (top - (all - top)) / all

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

  def disableWarning(): Unit = Logger.getLogger("org").setLevel(Level.OFF)

  def printPartizione[T](value: RDD[T]): Unit = {
    /*STAMPA IL CONTENUTO DI OGNI PARTIZIONE*/
    value.mapPartitionsWithIndex(
      (index, it) => it.toList.map(println(s"PARTIZIONE:${index}", _)).iterator
    ).collect()
  }

  def printLinksGeneral(links: RDD[(String, List[String])]):Unit = {
    // UTILIZZATA IN GENERAL MODE
    var list = links.collect()
    var jsonList = list.flatMap( u => {
      var userSource = u._1
      var targetList = u._2.filter( user => !user.eq(userSource)) //ogni lista ricevete contiente userSource che non deve essere considerato
      targetList.map(u => linkJson(userSource.replace("\"", ""), u.replace("\"", "")))
    })

    val jsonString = write(jsonList.distinct)(DefaultFormats)

    println("#linksInitial#"+jsonString)

  }

  def printLinksProd[T](lista: RDD[(String, (Iterable[User], Float, String))]): Unit = {
    println("Stampa links before iteration")
    // UTILIZZATA IN PRODUCT MODE
    /*Utilizzata per l'aggiornamento continuo per la view del grafo */
    var list = lista.collect()
    var jsonList = list.flatMap(u => {
      var idSource = u._1
      var targetList = u._2._1.filter(user => !(user.idUser).eq(u._1))
      targetList.map(u => linkJson(idSource.replace("\"", ""), u.idUser.replace("\"", "")))
    })
    val jsonString = write(jsonList.distinct)(DefaultFormats)

    println("#linksInitial#"+jsonString)
  }

  def getResult(partitionedRDD: RDD[(String, Iterable[User])], iter:Int):Unit = {
    if(iter==0){
      println("Stampa initial nodes")
    }else{
      println(s"Stampa nodes iter ${iter}")
    }

    /* Stampa in nodes.json il rank relativo ad ogni user */
    var ranks = partitionedRDD.flatMap { case (idArt, users) => users.map(user => user.idUser -> user.helpfulness) }.groupByKey()
    var result = ranks.map { case (idUser, listHelpful) => {
      var size = listHelpful.size
      var sumHelpful = listHelpful.foldLeft(0f) {
        case (acc, value) => acc + value
      }
      (idUser, sumHelpful / size)
    }
    }
    var jsonList = result.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
    printNodes(jsonList,iter)
  }

  def printResultRank(partitionedRDD: RDD[(String, Iterable[User])]): Unit = {
    var ranks = partitionedRDD.flatMap { case (idArt, users) => users.map(user => user.idUser -> user.helpfulness) }.groupByKey()
    var result = ranks.map { case (idUser, listHelpful) => {
      var size = listHelpful.size
      var sumHelpful = listHelpful.foldLeft(0f) {
        case (acc, value) => acc + value
      }
      (idUser, sumHelpful / size)
    }
    }
    result.collect().foreach(println("> ", _))
  }

  def printNodes(jsonList:Array[userJson],iter:Int): Unit = {
    val jsonString = write(jsonList)(DefaultFormats)
    if(iter == 0){
      println("#nodesInitial#"+jsonString)
    }else{
      println("#nodesIter"+iter+"#"+jsonString)
    }
  }

  def getTime(string: String,t0:Long) = {

    var yourmilliseconds = System.currentTimeMillis();
    var sdf = new SimpleDateFormat("HH:mm:ss");
    var resultdate = new Date(yourmilliseconds);
    println("> Tempo di " + string + ": " + (System.nanoTime()-t0)/1000000000f + "s \t\t ( " + sdf.format(resultdate) + " )")

  }

  /* PRODUCT */
  def startComputeProd(path: String, sc: SparkContext,LAMBDA: Int,ITER: Int, DEMO: Boolean, NUM_PARTITIONS: Int): Unit = {
    // Creazione RDD e partizione in base all'idArticolo

    val t_preload = System.nanoTime()
    val dataRDD = load_rdd(path, sc)
    var partitionedRDD = dataRDD.partitionBy(new CustomPartitioner(NUM_PARTITIONS,false)).persist()
    partitionedRDD.take(1).foreach(_ => getTime("load e part: ",t_preload))
    computeProd(partitionedRDD,LAMBDA,ITER,DEMO)
  }

  def computeProd(pRDD: RDD[(String, Iterable[User])], LAMBDA: Int, ITER: Int,demo: Boolean): Unit = {
    var partitionedRDD = pRDD
    var t0 = System.nanoTime()

    if (demo) getResult(partitionedRDD,0)
    for (i <- 1 to ITER) { // INIZIO ITER
      /* Struttura intermedia fatta in tal modo :
      *  (UtenteDonatore X, UtentiRiceventi,idArticolo)
      * dove UtentiRiceventi è l'insieme degli utenti con helpfulness minore di X ma che
      * hanno votato (rating) come X
      *
      * la lista utentiRiceventi comprende anche l'user stesso il cui contributo a se stesso
      * sarà pari a 0. Serve come supporto per le fasi successive
      */
      var orderLinks = partitionedRDD.flatMap { case (key, users) => users.map(p => (p, users.filter(
        refUser => (p.helpfulness > refUser.helpfulness || (p.idUser).eq(refUser.idUser)) && p.rating == refUser.rating), key
      ))
      }
      /*-------COLLEGAMENTI IN BASE AL VOTO E ALLA HELPFUL */
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


      /* Ora si calcolano le coppie (ricevente, contributo)
      ottenendo per ogni partizione la lista  di tale coppia <chiave,value>
      dove la chiave è il ricevente che deve sommare alla propria helpfulness
      un valore pari a value (contributo_ricevuto)
      * */
      val contribs = listaAdiacenza.flatMap {
        pair => { // per ogni coppia (Donatore,(ListaDest, myHelp)
          val currentId = pair._1
          val helpfulnessCurrent = pair._2._2
          val E = pair._2._1.size - 1 //ogni lista contiene anche il contributo che l'user deve dare a se stesso (ossia 0)
          //serve per ottenere la lista completa delle helpfulness
          val idArt = pair._2._3
          //     HowMuch? => myHelp/Lambda*|Nodi con helpfulness minori della mia|
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
        it.toList.groupBy(_._2._2).iterator
          .map( // 1.
            x =>
              x._1 -> //idArt, x._2 è la lista degli user
                x._2.groupBy(_._1)
                  .mapValues( // 2. raggruppamento per idUser appartenenti allo stesso articolo
                    user => { // e per ogni utente somma dei contributi e della helpfulness
                      var u = user.head //prendo il primo utente nella lista in quanto l'unico valore che varia
                    //è il contributo che verra accumulato dalla foldLeft per calcolare la nuova helpfulness
                    var userOldHelp = u._2._3 // helpfulness dell'user prima dell'update
                    var newValue = user.foldLeft(userOldHelp) { // user ha questa struttura -> (idUser,(contributo,idArticolo,helpful,rating)
                      case (acc, (idUser, (singleContr, idArt, help, rating))) => { // estraggo il valore del contributo e incremento acc
                        var newHelp = acc + singleContr // che inizialmente è helpfulnessUtente
                        if (newHelp > 1.0) 1.0f
                        else if (newHelp < -1.0) -1.0f
                        else newHelp
                      }
                    }
                      new User(u._1, u._2._4, newValue) //creo nuovo oggetto contenente le info necessarie per iniziare una nuova iterazione
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
    result.coalesce(1,shuffle = true).saveAsTextFile(pathOutput)


  }

  /* GENERAL */
  def load_rdd_commFORusr(path: String, sc: SparkContext): RDD[(String, Iterable[UserComment])] = {
    val header = sc.textFile(path).first()
    val new_record = sc.textFile(path).filter(row => row!= header)
    new_record.map(r => {
      val fields = r.split(",")
      var idProd = fields(1)
      var posVal = fields(2).substring(2).toFloat
      var allVal = fields(3).dropRight(2).substring(1).toFloat
      val initHelpfulness = if (allVal == 0) 0 else localHelpfulnessInit(posVal, allVal)
      var rating = fields(4).toInt
      var idUser = fields(5)
      (idUser, new UserComment(idProd, rating, initHelpfulness ))
    }).distinct().groupByKey()
  }

  def startComputeGeneral[T](path: String, sc: SparkContext, LAMBDA: Int, DEMO:Boolean, ITER: Int, NUM_PARTITIONS: Int): Unit = {
    println("------- Caricamento csv in RDD -------")
    var t0 = System.nanoTime()
    val commentsForUsers = load_rdd_commFORusr(path, sc)
    /* Fase 0 Partizione per idUtente */
    var rddCommForUsr = commentsForUsers.partitionBy(new CustomPartitioner(NUM_PARTITIONS, false))
    rddCommForUsr.take(1).foreach( _ => getTime("load e part",t0))
    /*Fase 1: Calcolo helpful locale (media delle helpfulness di ogni utente)
    * con successivo ragguppamento per idArticolo */

    t0 = System.nanoTime()
    val rddUserForProdNewHelpful = rddCommForUsr.flatMap {
      case (usr, usrCommts) => {
        val (hel, nhelp) = usrCommts.foldLeft((0f, 0)) {
          case ((acc1, acc2), comm) => {
            (acc1 + comm.helpfulness, acc2 + 1)
          }
        }
        var globalHelpful = hel / nhelp
        usrCommts.map(comm => (comm.idProd, usr, comm.rating, globalHelpful))
      }
    }
    rddUserForProdNewHelpful.take(1).foreach( _ => getTime("calcolo helpfulness locale (F1)",t0))

    /* Fase 2: (join) raggruppamento per idProd */
    t0 = System.nanoTime()
    var rddUserForProdGroup = rddUserForProdNewHelpful.groupBy(_._1).partitionBy(new CustomPartitioner(NUM_PARTITIONS, false))
    rddUserForProdGroup.take(1).foreach( _ => getTime("raggruppamento per idProd (F2)",t0))

    /* Fase 3: Calcolo link localmente
    * Determinare i nodi donatori e i nodi riceventi in base allo stesso rating e alle loro helpfulness (chi è maggiore dona) */
    t0 = System.nanoTime()
    var rddLocalLinkAndHelp = rddUserForProdGroup.flatMap {
      case (key, users) => {
        users.map(
          usr => {
            var idUser = usr._2
            var helpCurr = usr._4
            var ratCur = usr._3
            val usrRiceventi = users.filter(otherUsr => otherUsr._4 < helpCurr && otherUsr._3 == ratCur).map(_._2)
            // idUser deve dare un suo contributo che dipende da helpcurr alla lista usrRiceventi
            ((idUser, helpCurr) -> usrRiceventi)
          }
        )
      }
    }.groupBy(_._1).persist()
    rddLocalLinkAndHelp.take(1).foreach( _ => getTime("calcolo link localmente (F3)",t0))


    /* Fase 4: (join) raggruppamento per idUtente
    *  creazione link e rank
      Fase 4: Raggruppamento idUtente per avere l'insieme totale dei riceventi
      !! Vengono determinati i link e il rank degli utenti in base ai soli commenti
    */
    t0 = System.nanoTime()
    val links = rddLocalLinkAndHelp.map {
      case (usrHelp, list) => usrHelp._1 -> (usrHelp._1 :: list.flatMap(_._2).toSet.toList) //.toSet per rimuovere i duplicati
    }.persist()

    /*VECCHIO MODO POI CANCELLIAMO QUESTO*/
    /* utilizzavamo links1 come struttura intermedia */
//    links.collect().foreach(println)
//    val links = links1.map(p => p._1 -> p._1).join(links1).mapValues(p => List(p._1) ++ p._2 ).persist()
//    println("_______")
//    links.collect().foreach(println)
//    links.take(1).foreach(_ => getTime("calcolo dei links globali (F4)",t0))



    var ranks = rddLocalLinkAndHelp.map {
      case (usrHelp, list) => usrHelp._1 -> usrHelp._2
    }

    if (DEMO) {
      /*nel caso generale il link non cambia mai
      * la stampa in links.json viene fatta solo qui */
      printLinksGeneral(links)
      var jsonList = ranks.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
      printNodes(jsonList,0)
    }

    /* Fase 5: inizio pageRank */
    for (i <- 1 to ITER) {
      t0 = System.nanoTime()
      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, urank)) =>
          uLinks.map(t =>
            (t.toString, if (uLinks.size == 1 || t.toString.equals(u.toString)) 0f else Math.abs(urank) / ((uLinks.size - 1) * LAMBDA))
          )
      }

      var addition = contributions.reduceByKey((x, y) => x + y)
      ranks = ranks.leftOuterJoin(addition)
        .mapValues(valore => if ((valore._1 + valore._2.getOrElse(0f)) > 1f) 1f else valore._1 + valore._2.getOrElse(0f))

      ranks.take(1).foreach( _ => getTime("iterazione " + i,t0))

      if (DEMO) {
        /* Ad ogni iterata stampa ranks.json i valori  solo demo Mode*/
        var jsonList = ranks.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
        printNodes(jsonList,i)
        Thread.sleep(timeout)
      }
    }

    println(s"----- Save RESULT (Ranks) in ${pathOutput} -----")
    t0 = System.nanoTime()
    ranks.coalesce(1,shuffle=true).saveAsTextFile(pathOutput)
    getTime("Scrittura in file", t0)
  }
}
