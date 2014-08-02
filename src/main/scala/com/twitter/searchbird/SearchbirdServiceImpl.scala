package com.twitter.searchbird

import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.Executors
import scala.collection.mutable
import config._

class SearchbirdServiceImpl(config: SearchbirdServiceConfig) extends SearchbirdService.ThriftServer {
  val serverName = "Searchbird"
  val thriftPort = config.thriftPort
  override val tracerFactory = config.tracerFactory

  /**
   * These services are based on finagle, which implements a nonblocking server.  If you
   * are making blocking rpc calls, it's really important that you run these actions in
   * a thread pool, so that you don't block the main event loop.  This thread pool is only
   * needed for these blocking actions.  The code looks like:
   *
   *     val futurePool = new FuturePool(Executors.newFixedThreadPool(config.threadPoolSize))
   *
   *     def hello() = futurePool {
   *       someService.blockingRpcCall
   *     }
   *
   */

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

  def shutdown() = {
    super.shutdown(0.seconds)
  }
}
