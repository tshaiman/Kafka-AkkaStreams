package com.ts.producer

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import com.ts.processor.{InfoMessage, InfoMessageFormat}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import play.api.libs.json.Json

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Random, Success}

object SampleWriter extends App{
  implicit val system: ActorSystem = ActorSystem("sample-graph-writer")
  implicit val materializer = ActorMaterializer()
  implicit val mpWrites = InfoMessageFormat
  val rand = Random
  val topic  = "stream-in"

  val producerSettings =
    ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)

  def genKey(value: Int) = {
    value.toString.getBytes
  }

  def genMessage(value: Int) = {
    Json.toJson(InfoMessage(UUID.randomUUID().toString,s"some-data-$value",100,1000+value)).toString.getBytes
  }

  val done: Future[Done] =
    Source(1 to 10)
      .map(
        value =>
          new ProducerRecord[Array[Byte], Array[Byte]](
            topic,
            genKey(value),
            genMessage(value)
          )
      )
      .runWith(Producer.plainSink(producerSettings))

  implicit val ec: ExecutionContextExecutor = system.dispatcher
  done onComplete {
    case Success(_)   => println("Done"); system.terminate()
    case Failure(err) => println(err.toString); system.terminate()
  }


}
