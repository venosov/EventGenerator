package com.gschiavon.kafka.producer

import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.Random


object MultipleTopicEvent extends App with Init {

  sendMultipleTopicEvent()

  def sendMultipleTopicEvent() = {
    val json = """{"company": "databricks", "employees":200, "date": "2021-04-03 16:00:00"}"""
    println("Producing Company Data To Multiple Topics")
    Source
      .fromIterator[String](
        () => Iterator.continually(
          json
        ))
      .map((value: String) => {
        val topic = Seq("topic_x1", "topic_x2", "topic_x3")(Random.nextInt(3))
        Thread.sleep(500)
        new ProducerRecord[String, String](topic, value)
      })
      .runWith(Producer.plainSink(producerSettings))
  }
}