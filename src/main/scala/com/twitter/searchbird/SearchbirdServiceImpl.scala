package com.twitter.searchbird

import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util._
import java.util.concurrent.Executors
import scala.collection.mutable
import config._

class SearchbirdServiceImpl(config: SearchbirdServiceConfig, index: Index) extends SearchbirdService.ThriftServer {
  val serverName = "Searchbird"
  val thriftPort = config.thriftPort

  def get(key: String) = index.get(key)
  def put(key: String, value: String) = index.put(key, value) flatMap { _ => Future.Unit }
  def search(query: String) = index.search(query)

  def shutdown() = {
    super.shutdown(0.seconds)
  }
}
