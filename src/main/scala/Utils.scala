package Util

import java.text.SimpleDateFormat
import java.util.Date

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

/*CLASSI AUSILIARIE PER LA CREAZIONE DEL JSON*/
case class userJson(id: String, rank: Float)
case class linkJson(source: String, target: String)

case class User(idUser:String,rating:Int, helpfulness:Float)
case class UserComment(idProd:String,rating:Int,helpfulness:Float)


object Utils {

  def localHelpfulnessInit(top: Float, all: Float) = (top - (all - top)) / all
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


}
