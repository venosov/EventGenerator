package com.gschiavon.kafka.producer

import java.util.Date

import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.Random


case class PoliticalEvent(name: String, surname: String, age: Int, party: String, date: String)

object ElectionEvent extends App with Init {

  sendElectionData()

  def sendElectionData() = {
    val json = """{"name": "replaceName", "surname": "replaceSurname", "age": "replaceAge", "party": "replaceParty", "date": "replaceDate"}"""
    println("Producing Election Data")
    Source
      .fromIterator[String](
        () => Iterator.continually(
          json.replaceName().replaceSurname().replaceAge().replaceParty().replaceDate()
        ))
      .map((value: String) => {
        Thread.sleep(500)
        new ProducerRecord[String, String]("election", value)
      })
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
