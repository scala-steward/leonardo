package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.{IO}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.protobuf.Timestamp
import com.google.pubsub.v1.{ProjectSubscriptionName, ProjectTopicName, PubsubMessage}
import fs2.concurrent.InspectableQueue
import fs2.{Pipe, Stream}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.{Event, GooglePublisher, GoogleSubscriber, PublisherConfig, SubscriberConfig}
import org.broadinstitute.dsde.workbench.google2._

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

object T {
  implicit val cs = IO.contextShift(global)
  implicit val t = IO.timer(global)
  implicit def logger = Slf4jLogger.getLogger[IO]
  val projectTopicName = ProjectTopicName.of("broad-dsde-dev", "leonardo-pubsub-1580744296")

//  val path = "/Users/jcanas/firecloud-repos/leonardo/config/leonardo-account.json"
  val path = "/etc/leonardo-account.json"

  val printPipe: Pipe[IO, Event[Messagee], Unit] = in => in.evalMap(s =>
    for {
      _ <- Logger[IO].info(s"recieved event: ${s.msg}")
      _ <- IO(s.consumer.ack())
    } yield ()
  )

  val queue = InspectableQueue.bounded[IO, Event[Messagee]](100).unsafeRunSync()
  val config = SubscriberConfig(path, projectTopicName, 1 minute, None)

//  val printPipe: Pipe[IO, Event[Messagee], Unit] = in => in.evalMap(s => IO(println("processed "+s.toString)))

  def publish(msg: String) = {
    val config = PublisherConfig(path, projectTopicName, org.broadinstitute.dsde.workbench.google2.GoogleTopicAdminInterpreter.defaultRetryConfig)
    val pub = GooglePublisher.resource[IO](config)
    pub.use(x => (Stream.eval(IO.pure(msg)) through x.publish).compile.drain)
  }

  implicit val msgDecoder: Decoder[Messagee] = Decoder.forProduct1("msg")(Messagee)
  implicit val msgEncoder: Encoder[Messagee] = Encoder.forProduct1("msg")(x => Messagee.unapply(x))

  def subscribe() = {
    for {
      _ <- IO.pure("lol")
      sub = GoogleSubscriber.resource[IO, Messagee](config, queue)
      _ <- sub.use {
        s =>
          val stream = Stream(
            Stream.eval(s.start),
            queue.dequeue through printPipe
          ).parJoin(2)
          stream.compile.drain
      }
    } yield ()
  }

  def receiver(queue: fs2.concurrent.Queue[IO, Event[Messagee]]): MessageReceiver = new MessageReceiver() {
    override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
     logger.info("in custom receive message")
      val result = for {
        json <- io.circe.parser.parse(message.getData.toStringUtf8)
        message <- json.as[Messagee]
        _ <- queue.enqueue1(Event(message, None, Timestamp.getDefaultInstance, consumer)).attempt.unsafeRunSync()
      } yield ()

      result match {
        case Left(e) =>
         logger.info(s"failed to publish $message to internal Queue due to $e")
          consumer.nack() //pubsub will resend the message up to ackDeadlineSeconds (this is configed during subscription creation
        case Right(_) =>
         logger.info(s"succeeded in publish message ${message} to internal queue")
      }
    }
  }

  def getAndStartSubscriber(): Subscriber = {
    val subscription = ProjectSubscriptionName.of(config.projectTopicName.getProject, config.projectTopicName.getTopic)

    val subIO = credentialResource[IO](path).use { c =>
      val s = Subscriber
        .newBuilder(subscription, receiver(queue))
        .setCredentialsProvider(FixedCredentialsProvider.create(c)).build()
      IO(s)
    }

    val subscriber = subIO.unsafeRunSync()

    logger.info("in janky custom subscriber, starting it")
    subscriber.startAsync()

    subscriber
  }

  def subscribe2() = {
    getAndStartSubscriber()
    (queue.dequeue through printPipe)
  }
}

final case class Messagee(msg: String)
