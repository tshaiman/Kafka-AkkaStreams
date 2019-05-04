package com.ts.processor

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.concurrent.ExecutionContext
import scala.util.Random

object Main extends App{
  implicit val system: ActorSystem = ActorSystem("stream-sample-graph")
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val mpWrites: InfoMessageFormat.type = InfoMessageFormat
  implicit val ex:ExecutionContext = ExecutionContext.global
  val inTopic = "stream-in"
  val outTopic = "stream-out"

  val producerSettings =
    ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)

  val consumerSettings =
    ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)

  val graph = ProcessingGraph(consumerSettings,producerSettings,new FakeDatabase,inTopic,outTopic)

  graph.run()


}
