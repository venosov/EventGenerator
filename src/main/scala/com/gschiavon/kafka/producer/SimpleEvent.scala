package com.gschiavon.kafka.producer

import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord


object SimpleEvent extends App with Init {

  sendCompanyEvent()

  def sendCompanyEvent() = {
    val json = """{"company": "databricks", "employees":200, "date": "2021-04-03 16:00:00"}"""
    println("Producing Company Data")
    Source
      .fromIterator[String](
        () => Iterator.continually(
          json
        ))
      .map((value: String) => {
        Thread.sleep(500)
        new ProducerRecord[String, String]("company", value)
      })
      .runWith(Producer.plainSink(producerSettings))
  }
}