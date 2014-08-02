package com.twitter.searchbird

import scala.collection.mutable
import com.twitter.util._
import com.twitter.conversions.time
import com.twitter.logging.Logger
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec

trait Index {
  def get(key: String): Future[String]
  def put(key: String, value: String): Future[Unit]
  def search(key: String): Future[List[String]]
}

class ResidentIndex extends Index {
  val log = Logger.get(getClass)

  val forward = new mutable.HashMap[String, String]()
    with mutable.SynchronizedMap[String, String]
  val reverse = new mutable.HashMap[String, Set[String]]()
    with mutable.SynchronizedMap[String, Set[String]]

  def get(key: String) = {
    forward.get(key) match {
      case None =>
        log.debug("get %s: miss", key)
        Future.exception(SearchbirdException("No such key"))
      case Some(value) =>
        log.debug("get %s: hit", key)
        Future(value)
    }
  }

  //TODO: when a document is overwritten, we need to clean the reverse index
  def put(key: String, value: String) = {
    log.debug("put %s", key)

    //serialize updates
    synchronized {
      //clean reverse index
      reverse transform { (value, record) =>
        record - key
      }

      //populate index
      forward(key) = value

      //populate reverse index
      val uniqueTokens = value.split(" ").toSet
      uniqueTokens foreach { token =>
        val currentEntry = reverse.getOrElse(token, Set())
        reverse(token) = currentEntry + key
      }

    }

    Future.Unit
  }

  def search(query: String) = Future.value {
    val tokens = query.split(" ")
    val hits = tokens map { token => reverse.getOrElse(token, Set()) }
    val intersected = hits reduceLeftOption { _ & _ } getOrElse Set()
    intersected.toList
  }

}

class CompositeIndex(indicies: Seq[Index]) extends Index {
  require(!indicies.isEmpty)

  def get(key: String) = {
    val queries = indicies.map { idx =>
      idx.get(key) map { result =>
        Some(result)
      } handle { case e =>
        None
      }
    }

    //flatMap? looks like applying a sync api, so assuming this is to flatten
    //out empty results if an index returns a blank list?
    //collect spits out a Future of sequence
    Future.collect(queries) flatMap { results =>
      //pick the first result that is defined (i.e. non None)
      //if none are defined, find with return None and we throw exception. YES.
      results.find { _.isDefined } map { _.get } match {
        case Some(v) => Future.value(v)
        case None => Future.exception(SearchbirdException("No such key"))
      }
    }
  }

  def put(key: String, value: String) = Future.exception(SearchbirdException("put() not supported by CompositeIndex"))

  def search(query: String) = {
    val queries = indicies.map { _.search(query) rescue { case _ => Future.value(Nil)  } }
    //why map here and not flatMap? Flatten is seperate...
    //I can see here we want a single list, not a list of lists, so we flatten
    Future.collect(queries) map { results =>
      (Set() ++ results.flatten ) toList
    }
  }
}

class RemoteIndex(hosts: String) extends Index {
  val transport = ClientBuilder()
    .name("removeIndex")
    .hosts(hosts)
    .codec(ThriftClientFramedCodec())
    .hostConnectionLimit(1)
    //.timeout(500.milliseconds)
    .build()
  val client = new SearchbirdService.FinagledClient(transport)

  def get(key: String) = client.get(key)
  def put(key: String, value: String) = client.put(key, value) map { _ => () }
  def search(query: String) = client.search(query) map { _.toList }
}
