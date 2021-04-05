package com.gschiavon.kafka.producer

import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import scala.collection.JavaConverters._


object HeadersEvent extends App with Init {

  sendEventWithHeaders()

  def sendEventWithHeaders() = {
    val json = """{"name": "replaceName", "surname": "replaceSurname", "age": "replaceAge", "party": "replaceParty", "date": "replaceDate"}"""
    println("Producing Election Data")
    Source
      .fromIterator[String](
        () => Iterator.continually(
          json
        ))
      .map((value: String) => {
        Thread.sleep(500)
        new ProducerRecord[String, String](
          "test", 0, "key", "value", Seq[Header](new RecordHeader("key", "localhost".getBytes())).asJava)
      })
      .runWith(Producer.plainSink(producerSettings))
  }
}
