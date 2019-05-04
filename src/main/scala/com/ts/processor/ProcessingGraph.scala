package com.ts.processor

import java.util.concurrent.atomic.AtomicLong

import akka.{ Done, NotUsed }
import akka.stream.scaladsl._
import akka.kafka.{
  ConsumerSettings => AkkaConsumerSettings,
  ProducerSettings => AkkaProducerSettings,
  _
}
import akka.kafka.scaladsl._
import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffsetBatch }
import akka.kafka.ProducerMessage.{ Message, Result }
import KafkaUtils._
import akka.stream.SourceShape
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json._

import scala.collection.immutable.List
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object ProcessingGraph {
  // We'll always be using byte arrays with Kafka, so there's no point in carrying around the
  // big types provided by the library. We'll fix everything to byte arrays.
  type ConsumerSettings = AkkaConsumerSettings[Array[Byte], Array[Byte]]
  type ProducerSettings = AkkaProducerSettings[Array[Byte], Array[Byte]]
  type KafkaMessage = CommittableMessage[Array[Byte], Array[Byte]]
  type ProducerMessage[P] = Message[Array[Byte], Array[Byte], P]
  type ProducerResult[P] = Result[Array[Byte], Array[Byte], P]

  implicit val resultFormat = ResultFormat

  private def kafkaConsumer(
    consumerSettings: ConsumerSettings,
    clientId: String,
    groupId: String,
    topic: String
  ): Source[KafkaMessage, Consumer.Control] = {

    val consumerSettingsWithIds =
      consumerSettings.withClientId(clientId).withGroupId(groupId)

    Consumer
      .committableSource(consumerSettingsWithIds, Subscriptions.topics(topic))

  }

  def toProducerMessage(result: InfoResult, destTopic: String): ProducerMessage[Unit] = {
    val messageResult = Json.toJson(result).toString.getBytes()
    val key = result.correlationId.getBytes
    ProducerMessage.Message(
      new ProducerRecord[Array[Byte], Array[Byte]](destTopic,key, messageResult), ())
  }

  private def deserializeFlow(organizationName: String) =
    Flow[KafkaMessage].map { msg =>
      deserializeKafkaMessage(organizationName, msg.record.value, msg.record.offset,msg.record.partition)
    }

  private def serializeFlow(
    destTopic: String,
  ): Flow[Option[BaseResult], Option[ProducerMessage[Unit]], NotUsed] =
    Flow[Option[BaseResult]].map {
      case Some(result) =>
        result match {
          case x: InfoResult => Some(toProducerMessage(x, destTopic))
        }
      case _ => None
    }

  private def enrichFlow(orgnaizationName: String)(implicit ec: ExecutionContext) =
    Flow[Option[InfoMessage]].map(info => {
      info.collect {
        case v: InfoMessage => v.copy(data = v.data.toUpperCase + "." + orgnaizationName)
      }
    });

  private def deserializeKafkaMessage(
    organizationName: String,
    bytes: Array[Byte],
    offset: Long,
    partition:Long
  ) = {
    implicit val infoMessageFormat = InfoMessageFormat
    messageDeserializer[InfoMessage].apply(bytes, offset) match {
      case Failure(e) =>
        println(
          e,
          "Failed to deserialize input",
          "organization" -> organizationName,
          "offset"       -> offset,
          "partition"    -> partition
        )
        None
      case Success(value) => Some(value)
    }
  }

  private def validationFlow(): Flow[Option[InfoMessage], Option[BaseResult], NotUsed] =
    Flow[Option[InfoMessage]].map { info =>
      info.collect {
        case v: InfoMessage =>
          InfoResult.fromMessage(v) match {
            case Right(res) => res
            case Left(err) => {
              println("Vaidation failed.")
              ErrorResult(err)
            }
          }
      }
    }

  private def saveDbFlow(db: Database)(implicit ec: ExecutionContext): Flow[Option[BaseResult], Option[InfoResult], NotUsed] =
    Flow[Option[BaseResult]].mapAsync(5) {
      case Some(value) => value match {
        case ir: InfoResult => db.write(ir)
      }
    }

  private def offsetCommitter(
    organizationName: String,
    batchSize: Long,
    rawInputsTopic: String,
  )(implicit
    ec: ExecutionContext
  ) =
    Flow[KafkaMessage]
      .batch(batchSize, List(_)) {
        case (msgs, msg) => {
          msg :: msgs
        }
      }
      .map { msgs =>
        val reversed = msgs.reverse
        val offsetBatch =
          reversed
            .map(_.committableOffset)
            .foldLeft(CommittableOffsetBatch.empty)(_ updated _)

        (reversed, offsetBatch)
      }
      .map {
        case (msgs, offsetBatch) =>
          /*for {
            (m, offset) <- offsetBatch.offsets.find(_._1.topic == rawInputsTopic)

          } println(s"commiting offset $offset, partition = ${m.partition}")*/
          val commitF = Future {
            offsetBatch.commitScaladsl()
          }
          commitF
            .map(_ => msgs)
      }
  //.mapConcat(identity)
  private def filterNonDeserializedMessages(rawInputsTopic: String, organizationName: String) =
    Flow[(Option[ProducerMessage[Unit]], KafkaMessage)].mapConcat {
      case (Some(producerMessage), msg) =>
        List(producerMessage.copy(passThrough = msg))
      case (None, _) =>
        println(
          "Some inputs failed in deserialization",
          "topic"        -> rawInputsTopic,
          "organization" -> organizationName)
        List()
    }
  private def kafkaProducer[P](
    producerSettings: ProducerSettings
  ): Flow[ProducerMessage[P], ProducerResult[P], NotUsed] =
    Producer.flow[Array[Byte], Array[Byte], P](producerSettings)

  private def producer(producerSettings: ProducerSettings) =
    kafkaProducer[KafkaMessage](producerSettings).map { result =>
      result.passThrough
    }

  def entireFlow(
    consumerSettings: ConsumerSettings,
    producerSettings: ProducerSettings,
    db: Database,
    inTopic: String,
    outTopic: String,
    clientId:String,
    consumerGroup:String
  )(implicit
    ex: ExecutionContext
  ) = {
    implicit val messageFormat = InfoMessageFormat

    val orgName = "tshaiman"
    val consumer =
      kafkaConsumer(consumerSettings, clientId, consumerGroup, inTopic).named("consumer")

    Source
      .fromGraph(GraphDSL.create(consumer) { implicit b => materializedConsumer =>
        import GraphDSL.Implicits._

        val deserialize = deserializeFlow(orgName)
          .named("deserializer")

        val committer = b.add(offsetCommitter(orgName, 3, inTopic))
        val serializer = serializeFlow(outTopic).named("serializer")
        val producerInstance = producer(producerSettings)
        val enrich = enrichFlow(orgName).named("enrichment")
        val validate = validationFlow().named("validation")
        val save = saveDbFlow(db).named("save")
        val filter = b.add(filterNonDeserializedMessages(inTopic, orgName))


        val bcast = b.add(Broadcast[KafkaMessage](2))
        val zip = b.add(Zip[Option[ProducerMessage[Unit]], KafkaMessage]())

        materializedConsumer ~> bcast.in; bcast.out(0) ~> deserialize ~> enrich ~> validate ~> save ~> serializer ~> zip.in0
                                          bcast.out(1)                                                            ~> zip.in1

        zip.out ~> filter ~> producerInstance ~> committer
        SourceShape(committer.out)
      })
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
    entireFlow(consumerSettings, producerSettings, db, inTopic, outTopic,"ts-stream-demo","grp-6").toMat(Sink.ignore)(
      Keep.both)
}
