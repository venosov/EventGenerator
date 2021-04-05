package com.gschiavon.kafka.producer

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._
import scala.util.Random


case class ElectionEvent(name: String, surname: String, age: Int, party: String, date: String)

object Event extends App {

  implicit val system: ActorSystem = ActorSystem()
  val config = system.settings.config.getConfig("akka.kafka.producer")
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

  electionData()

  def electionData() = {
    val json = """{"name": "replaceName", "surname": "replaceSurname", "age": "replaceAge", "party": "replaceParty", "date": "replaceDate"}"""
    println("Producing Election Data")
    Source
      .fromIterator[String](
        () => Iterator.continually(
          json.replaceName().replaceSurname().replaceAge().replaceParty().replaceDate()
        ))
      .map((value: String) => {
        Thread.sleep(500)
        new ProducerRecord[String, String]("clicks", value)
      })
      .runWith(Producer.plainSink(producerSettings))
  }

  def eventWithHeaders() = {
    val json = """{"name": "replaceName", "surname": "replaceSurname", "age": "replaceAge", "party": "replaceParty", "date": "replaceDate"}"""
    println("Producing Election Data")
    Source
      .fromIterator[String](
        () => Iterator.continually(
          json.replaceName().replaceSurname().replaceAge().replaceParty().replaceDate()
        ))
      .map((value: String) => {
        Thread.sleep(500)
        new ProducerRecord[String, String](
          "test", 0, "key", "value", Seq[Header](new RecordHeader("key", "localhost".getBytes())).asJava)      })
      .runWith(Producer.plainSink(producerSettings))
  }


  implicit class Replace(json: String) {
    def replaceName(): String = {
      val names = Seq("Juan", "Pedro", "Gustavo")
      json.replace("replaceName", names(Random.nextInt(3)))
    }

    def replaceDate(): String = {
      json.replace("replaceDate", dateFormat.format(new Date()))
    }

    def replaceSurname(): String = {
      val names = Seq("Tower", "Smith", "Johnson")
      json.replace("replaceName", names(Random.nextInt(3)))
    }

    def replaceAge(): String = {
      json.replace("replaceName", Random.nextInt(100).toString)
    }

    def replaceParty(): String = {
      val parties = Seq("Republican", "Democrat", "Libertarian", "Green Party", "Constitution Party")
      json.replace("replaceName", parties(Random.nextInt(5)))
    }
  }
}
