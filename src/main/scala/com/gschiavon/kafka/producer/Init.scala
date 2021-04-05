package com.gschiavon.kafka.producer

import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import org.apache.kafka.common.serialization.StringSerializer

trait Init {
  implicit val system: ActorSystem = ActorSystem()
  val config = system.settings.config.getConfig("akka.kafka.producer")
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
}
