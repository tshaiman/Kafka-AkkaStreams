package com.ts.processor

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.stream.scaladsl._
import akka.kafka.{
  ConsumerSettings => AkkaConsumerSettings,
  ProducerSettings => AkkaProducerSettings,
  _
}
import akka.kafka.scaladsl._
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json._
import scala.concurrent.{ ExecutionContext, Future }

object ProcessingGraph {
  // We'll always be using byte arrays with Kafka, so there's no point in carrying around the
  // big types provided by the library. We'll fix everything to byte arrays.
  type ConsumerSettings = AkkaConsumerSettings[Array[Byte], Array[Byte]]
  type ProducerSettings = AkkaProducerSettings[Array[Byte], Array[Byte]]
  type CommittableMessage =
    ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]]
  type ProducerMessage[P] = ProducerMessage.Message[Array[Byte], Array[Byte], P]
  type ProducerResult[P] = ProducerMessage.Result[Array[Byte], Array[Byte], P]

  implicit val resultFormat = ResultFormat

  def toProducerMessage(
    kafkaMsg: CommittableMessage,
    result: Result,
    destTopic: String
  ): ProducerMessage[CommittableMessage] = {

    val messageResult = Json.toJson(result).toString.getBytes()
    ProducerMessage.Message(
      new ProducerRecord[Array[Byte], Array[Byte]](destTopic, 0, Array[Byte](), messageResult),
      kafkaMsg)

  }

  def entireFlow(
    consumerSettings: ConsumerSettings,
    producerSettings: ProducerSettings,
    db: Database,
    inTopic: String,
    outTopic: String
  )(implicit
    ex: ExecutionContext
  ): Source[Done, Consumer.Control] = {
    implicit val messageFormat = MessageFormat
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(inTopic))
      // JSON deserialization
      .mapConcat { kafkaMsg =>
        Json.parse(kafkaMsg.record.value).validate[Message] match {
          case JsSuccess(v, _) => List((kafkaMsg, v))
          case e: JsError      => Nil
        }
      }
      // Message validation
      .mapConcat {
        case (kafkaMsg, msg) =>
          Result.fromMessage(msg) match {
            case Right(result) => List((kafkaMsg, result))
            case Left(err)     => Nil
          }
      }

      // Database write
      .mapAsync(1) {
        case (kafkaMsg, result) =>
          db.write(result).map(_ => (kafkaMsg, result))
      }
      // ProducerMessage creation
      .map {
        case (kafkaMsg, result) => toProducerMessage(kafkaMsg, result, outTopic)
      }
      // Topic output
      .via(Producer.flexiFlow(producerSettings))
      // Message commit
      .mapAsync(1)(_.passThrough.committableOffset.commitScaladsl())

  }

  def apply(
    consumerSettings: ConsumerSettings,
    producerSettings: ProducerSettings,
    db: Database,
    inTopic: String,
    outTopic: String
  )(implicit
    ex: ExecutionContext
  ): RunnableGraph[(Consumer.Control, Future[Done])] =
    entireFlow(consumerSettings, producerSettings, db, inTopic, outTopic).toMat(Sink.ignore)(
      Keep.both)
}
