package Utils

import java.io._

import classes.{CustomPartitioner, User, UserComment}
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*CLASSI AUSILIARIE PER LA CREAZIONE DEL JSON*/
case class userJson(id: String, rank: Float)
case class linkJson(source: String, target: String)
object Util {

  var pathNodes = "../GUI/public/nodes.json"
  var pathLinks = "../GUI/public/links.json"
  var timeout = 5000

  def localHelpfulnessInit(top: Float, all: Float) = (top - (all - top)) / all
  /*Utlizzate in computeProd per caricare il csv*/
  def load_rdd(path: String, sc: SparkContext): RDD[(String, Iterable[User])] = {
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
  def linkedListToJsonGeneral(links: RDD[(String, List[String])]) = {
    println(s"Stampa links.js -> ${pathLinks}")

    // UTILIZZATA IN GENERAL MODE
    var list = links.collect()
    var jsonList = list.flatMap( u => {
      var userSource = u._1
      var targetList = u._2.filter( user => !user.eq(userSource)) //ogni lista ricevete contiente userSource che non deve essere considerato
      targetList.map(u => linkJson(userSource.replace("\"", ""), u.replace("\"", "")))
    })
    val jsonString = write(jsonList.distinct)(DefaultFormats)
    val pw = new PrintWriter(new File(pathLinks))
    pw.write(jsonString)
    pw.close()
  }

  def linkedListToJson[T](lista: RDD[(String, (Iterable[User], Float, String))]) = {
    println(s"Stampa links.js -> ${pathLinks}")
    // UTILIZZATA IN PRODUCT MODE
    /*Utilizzata per l'aggiornamento continuo per la view del grafo */
    var list = lista.collect()
    var jsonList = list.flatMap(u => {
      var idSource = u._1
      var targetList = u._2._1.filter(user => !(user.idUser).eq(u._1))
      targetList.map(u => linkJson(idSource.replace("\"", ""), u.idUser.replace("\"", "")))
    })
    val jsonString = write(jsonList.distinct)(DefaultFormats)
    val pw = new PrintWriter(new File(pathLinks))
    pw.write(jsonString)
    pw.close()
  }

  def getResult(partitionedRDD: RDD[(String, Iterable[User])]) = {
    println(s"Stampa nodes.js -> ${pathNodes}")

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
    writeRankFile(jsonList)
  }

  def printResultRank(partitionedRDD: RDD[(String, Iterable[User])]) = {

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

  def writeRankFile(jsonList:Array[userJson]) = {
    val jsonString = write(jsonList)(DefaultFormats)
    val pw = new PrintWriter(new File(pathNodes))
    pw.write(jsonString)
    pw.close()
  }

  def startComputeProd(path: String, sc: SparkContext,LAMBDA: Int,ITER: Int, DEMO: Boolean, DEBUG: Boolean, NUM_PARTITIONS: Int) = {
    // Creazione RDD e partizione in base all'idArticolo
    val dataRDD = load_rdd(path, sc)
    /*
    * TODO: Raffinare il partizionamento in modo da avere un bilanciamento dei dati
    * CustomPartioner è una classe creata ad hoc per tale scopo
    * codice in classes.CustomerPartitioner
    * DEBUG = true -> stampa la locazione dei dati in base all'id dell'articolo
    * */
    //    val mapProdElem = dataRDD.map( elem => (elem._1,elem._2.toList.length)).collectAsMap()
    //    var partitioner = new CustomPartitioner(NUM_PARTITIONS,true, mapProdElem)
    var partitioner = new CustomPartitioner(NUM_PARTITIONS,true)
    var partitionedRDD = dataRDD.partitionBy(partitioner).persist()
    computeProd(partitionedRDD,LAMBDA,ITER,DEBUG,DEMO)
  }
  def computeProd(pRDD: RDD[(String, Iterable[User])], LAMBDA: Int, ITER: Int, DEBUG: Boolean, demo: Boolean): Unit = {
    var partitionedRDD = pRDD

    if (DEBUG) {
      println("------ PRIMA DELLE ITERAZIONI -----")
      partitionedRDD.flatMap { case (idArt, users) => users.map(user => user.idUser -> user.helpfulness) }.groupByKey().collect().foreach(println)
    }
    /* Prima dell'inizio delle iterazioni vengono stampati i rank per calcolati in base alla media */
    if (demo){
      println("> stampa stato iniziale dei nodi")
      getResult(partitionedRDD)
    }



    // INIZIO ITER
    for (i <- 1 to ITER) {

      println(s"> INIZIO ITERAZIONE NUMERO -> ${i}")

      /* Struttura intermedia fatta in tal modo :
      *  (UtenteDonatore X, UtentiRiceventi,idArticolo)
      * dove UtentiRiceventi è l'insieme degli utenti con helpfulness minore di X ma che
      * hanno votato (rating) come X
      *
      * la lista utentiRiceventi comprende anche l'user stesso il cui contributo a se stesso
      * sarà pari a 0. Serve come supporto per le fasi successive
      */
      var orderLinks = partitionedRDD.flatMap { case (key, users) => users.map(p => (p, users.filter(
        refUser => ((p.helpfulness > refUser.helpfulness || (p.idUser).eq(refUser.idUser)) && p.rating == refUser.rating)), key
      ))
      }
      if (DEBUG) println("-------COLLEGAMENTI IN BASE AL VOTO E ALLA HELPFUL-------------")
      if (DEBUG) printPartizione(orderLinks) //in Util.scala
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
      if (demo) linkedListToJson(listaAdiacenza)
      if (demo) Thread.sleep(timeout) //tempo al client di aggiornare la view
      if (DEBUG) println("-------LISTA DI ADIACENZA-------------")
      if (DEBUG) printPartizione(listaAdiacenza) //in Util.scala

      /* Ora si calcolano le coppie (ricevente, contributo)
      ottenendo per ogni partizione la lista  di tale coppia <chiave,value>
      dove la chiave è il ricevente che deve sommare alla propria helpfulness
      un valore pari a value (contributo_ricevuto)
      * */
      if (DEBUG) println("----Contributi (y,(value, idArt, helpfull di y)) y deve ricevere un contrib pari a value per aver commentato lo stesso idArt -------------")
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
      if (DEBUG) printPartizione(contribs) //in Util.scala

      /* (1.) Raggruppamento per articolo Y per ogni partizione in quanto ci possono essere più articoli
      * nella stessa partizione
      *  (2.) Raggruppamento per idUtente e calcolo la somma dei contributi
      *  per l'utente X relativo all'articolo Y sommando con la helpfulness */
      if (DEBUG) println("--------SOMMA CONTRIBS E Helpfulness PER USER (stesso articolo)------------")
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

      if(demo) getResult(partitionedRDD) //ad ogni fine iterazioni viene aggiornato il file nodes.json

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
    if (DEBUG) println("---------------------")
    if(!demo) {
      var result = partitionedRDD.persist()
      printResultRank(result)
      //se non è demo stampa su file solo l'ultima iterata ossia quando il ciclo è finito
      getResult(result)

    }

  }

  /* ALL IN ONE */

  def load_rdd_commFORusr(path: String, sc: SparkContext): RDD[(String, Iterable[UserComment])] = {
    sc.textFile(path).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.map(r => {

      val fields = r.split(",")
      var idProd = fields(1)
      var posVal = fields(2).substring(2).toFloat
      var allVal = fields(3).dropRight(2).substring(1).toFloat
      val initHelpfulness = if (allVal == 0) 0 else localHelpfulnessInit(posVal, allVal)
      var rating = fields(4).toInt
      var idUser = fields(5)

      //map return
      (idUser, new UserComment(idProd, rating, initHelpfulness))

    }).groupByKey()
  }

  def startComputeGeneral[T](path: String, sc: SparkContext, LAMBDA: Int, DEMO:Boolean, ITER: Int, DEBUG: Boolean, NUM_PARTITIONS: Int): Unit = {
    println("------- Caricamento csv in RDD -------")
    val commentsForUsers = load_rdd_commFORusr(path, sc)

    /* Fase 0 Partizione per idUtente */
    if(DEBUG) println("------ Fase 0: Partizione per idUtente ------")
    var rddCommForUsr = commentsForUsers.partitionBy(new CustomPartitioner(NUM_PARTITIONS, DEBUG)).persist()
    if (DEBUG) printPartizione(rddCommForUsr)
    /*Fase 1: Calcolo helpful locale (media delle helpfulness di ogni utente)
    * con successivo ragguppamento per idArticolo */
    if (DEBUG) println("------- Fase 1: Calcolo Helpfulness localmente --------")
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
    if (DEBUG) printPartizione(rddUserForProdNewHelpful)
    /* Fase 2: (join) raggruppamento per idProd */

    if (DEBUG) println("------- Fase 2: Raggruppamento per idProd -------")
    var rddUserForProdGroup = rddUserForProdNewHelpful.groupBy(_._1).partitionBy(new CustomPartitioner(NUM_PARTITIONS, DEBUG))
    if (DEBUG) {
      printPartizione(rddUserForProdGroup)
      println("-------------------------")

    }


    /* Fase 3: Calcolo link localmente
    * Determinare i nodi donatori e i nodi riceventi inbase allo stesso rating e alle loro helpfulness (chi è maggiore dona) */
    if (DEBUG) println("------- Fase 3: Calcolo link localmente -------")
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
    }.groupBy(_._1)

    if (DEBUG) {
      printPartizione(rddLocalLinkAndHelp)
      println("----------------------------")
    }


    /* Fase 4: (join) raggruppamento per idUtente
    *  creazione link e rank*/
    if (DEBUG)  {
      println("------- Fase 4: Raggruppamento idUtente per avere l'insieme totale dei riceventi ----------")
      println("------- !!! Vengono determinati i link e il rank degli utenti in base ai soli commenti ---------")
    }

    val links1 = rddLocalLinkAndHelp.map {
      case (usrHelp, list) => usrHelp._1 -> list.flatMap(_._2)
    }.persist()
    val links2 = links1.map(p => p._1 -> p._1)
    val links = links2.join(links1).mapValues(p => List(p._1) ++ p._2 )

    var ranks = rddLocalLinkAndHelp.map {
      case (usrHelp, list) => {
        usrHelp._1 -> usrHelp._2
      }
    }
    if (DEBUG) {
      println("------ Link (X,List(....)) ossia X deve dare agli elementi della lista ------")
      printPartizione(links)
      println("------ Rank (prima delle partizioni) ------")
      printPartizione(ranks)
      println("------ END Fase 4 ------")
    }
    if (DEMO) {
      /*nel caso generale il link non cambia mai
      * la stampa in links.json viene fatta solo qui */
      linkedListToJsonGeneral(links)
    }

    /* Fase 5: inizio pageRank */
    if (DEBUG) println("------- Fase 5: Inizio PageRankCustomized -------")

    for (i <- 1 to ITER) {
      /* Ad ogni iterata stampa ranks.json i valori  solo demo Mode*/
      if (DEMO) {
        var jsonList = ranks.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
        writeRankFile(jsonList)
        /* end link.json AND ranks.json*/
        Thread.sleep(timeout)
      }
      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, urank)) =>
          uLinks.map(t =>
            (t.toString, if (uLinks.size == 1 || t.toString.equals(u.toString)) 0f else Math.abs(urank) / ((uLinks.size - 1) * LAMBDA))
          )
      }
      var addition = contributions.reduceByKey((x, y) => x + y)
      ranks = ranks.leftOuterJoin(addition)
        .mapValues(valore => if ((valore._1 + valore._2.getOrElse(0f)) > 1f) 1f else valore._1 + valore._2.getOrElse(0f))
    }
    //stampa dell'ultima iter che sia demo oppure no va fatta!
    var jsonList = ranks.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
    writeRankFile(jsonList)
    if (DEBUG) printPartizione(ranks)
    println("----- RESULT (Ranks) -----")
    ranks.collect().foreach(println(">",_))

  }
}
