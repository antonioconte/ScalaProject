package Rank

import Custom.CustomPartitioner
import Util.Utils._
import Util.{UserComment, userJson}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RankGeneral {

  var pathOutput = ""
  var timeout = 0
  def setPath(path : String, t: Int) ={
    pathOutput = path
    timeout = t
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

  def start[T](path: String, sc: SparkContext, LAMBDA: Int, DEMO:Boolean, ITER: Int, NUM_PARTITIONS: Int): Unit = {
    println("------- Caricamento csv in RDD -------")
    var t0 = System.nanoTime()
    val commentsForUsers = load_rdd_commFORusr(path, sc)
    /* Fase 0 Partizione per idUtente */
    var rddCommForUsr = commentsForUsers.partitionBy(new CustomPartitioner(NUM_PARTITIONS, false))
    rddCommForUsr.count()
    getTime("load e part",t0)
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
    rddUserForProdNewHelpful.count()
    getTime("calcolo helpfulness locale (F1)",t0)

    /* Fase 2: (join) raggruppamento per idProd */
    t0 = System.nanoTime()
    var rddUserForProdGroup = rddUserForProdNewHelpful.groupBy(_._1).partitionBy(new CustomPartitioner(NUM_PARTITIONS, false))

    rddUserForProdGroup.count()
    getTime("raggruppamento per idProd (F2)",t0)

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
            (idUser, helpCurr) -> usrRiceventi
          }
        )
      }
    }.groupBy(_._1).persist()
    rddLocalLinkAndHelp.count()
    getTime("calcolo link localmente (F3)",t0)

    /* Fase 4: (join) raggruppamento per idUtente e creazione link e rank
      Raggruppamento idUtente per avere l'insieme totale dei riceventi
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



    var ranks = rddLocalLinkAndHelp.map {
      case (usrHelp, list) => usrHelp._1 -> usrHelp._2
    }

    if (DEMO) {
      printLinksGeneral(links)
      var jsonList = ranks.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
      printNodes(jsonList,0)
    }

    /* Fase 5: inizio pageRank */
    for (i <- 1 to ITER) {
      t0 = System.nanoTime()
      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, urank)) =>
          /*u deve dare un contributo agli utenti in uLinks */
          uLinks.map(t =>
            /*per ogni utente in uLinks si calcola quanto deve ricevere da u il cui rank è urank
            * se uLinks == 1 significa che l'utente non deve dare nulla in quanto l'unico utente in uLinks è lui stesso
            * altrimenti viene diviso uRank in base al numero di utente riceventi meno l'user donatore */
            (t.toString, if (uLinks.size == 1 || t.toString.equals(u.toString)) 0f else Math.abs(urank) / ((uLinks.size - 1) * LAMBDA))
          )
      }
      /*contributions è una map <K,V> e si vanno a sommare tutti i contributi per ogni utente*/
      var addition = contributions.reduceByKey((x, y) => x + y)
      /*si aggiornano i rank sommando i contributi*/
      ranks = ranks.leftOuterJoin(addition)
        .mapValues(valore => if ((valore._1 + valore._2.getOrElse(0f)) > 1f) 1f else valore._1 + valore._2.getOrElse(0f))

      ranks.count()
      getTime("iterazione " + i,t0)

      if (DEMO) {
        var jsonList = ranks.collect().map(u => userJson(u._1.replace("\"", ""), u._2))
        printNodes(jsonList,i)
        Thread.sleep(timeout)
      }
    }

    /* stampa del risultato su directory output */
    t0 = System.nanoTime()
    ranks.coalesce(1,shuffle=true).saveAsTextFile(pathOutput)
    println(s"----- Save RESULT (Ranks) in ${pathOutput} -----")
    getTime("Scrittura File", t0)
  }
}
