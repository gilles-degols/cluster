package net.degols.libs.cluster

import java.io.{PrintWriter, StringWriter}

import akka.actor.ActorRef
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import sys.process._

/**
  * Created by Gilles.Degols on 04-09-18.
  */
object ClusterTools {
  def datetime(): DateTime = new DateTime().withZone(DateTimeZone.UTC)

  def difference(otherDatetime: DateTime): Long = otherDatetime.getMillis - datetime().getMillis

  def formatStacktrace(exception: Throwable, keepPackages: Seq[String] = List.empty[String]): String = {
    val sw = new StringWriter()
    exception.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  /**
    * Convert a list of future to a list of futures with Try. Typically useful to use alongside with Future.sequence(...)
    * @param f
    * @tparam T
    * @return
    */
  def futureToFutureTry[T](f: Future[T])(implicit ec: ExecutionContext): Future[Try[T]] = {
    f.map(Success(_)).recover{ case x => Failure(x)}
  }


  /**
    * Fold an iterator of futures together, to process them one by one
    */
  def foldFutures[A, T](iter: Iterator[T], met: T => Future[A], stopOnFailure: Boolean = false)(implicit ec: ExecutionContext): Future[Seq[Try[A]]] = {
    val resBase = Future.successful(mutable.ListBuffer.empty[Try[A]])
    var failureSeen: Boolean = false

    iter.foldLeft(resBase) { (futureRes, current) =>
      if(!failureSeen || !stopOnFailure) {
        futureRes.flatMap(res => {
          // By wrapping the "met" with a future, we handle a potential problem in the "met" outside of the expected Future itself
          Future{met(current)}
            .flatten
            .transform {
              case Success(r) => Success(Try{r})
              case Failure(e) =>
                failureSeen = true
                Success(Try{throw e})
            }.map(raw => {
              res.append(raw)
              res
            })
        })
      } else {
       futureRes
      }
    }.map(_.toList)
  }

  /**
    * Return the full path to a given actor ref
    * @param actorRef
    */
  def remoteActorPath(actorRef: ActorRef): String = {
    val path = akka.serialization.Serialization.serializedActorPath(actorRef).split("#").head
    if(!path.contains("@")) {
      throw new Exception(s"Missing configuration to have a valid remote actor path for $actorRef.")
    }
    path
  }

  /**
    * Verify if two given actorRef belongs to the same JVM. It is useful to send some specific messages for performance
    * purpose (for example, sending an object reference directly instead of asking the process to reload the same
    * object from a db).
    * We use the hostname + port to see if it's the same jvm, nothing more complicated.
    */
  def actorsFromSameJVM(sender: ActorRef, receiver: ActorRef): Boolean = {
    val senderPath = jvmIdFromActorRef(sender).split("//").head.split("/").head
    val receiverPath = jvmIdFromActorRef(receiver).split("//").head.split("/").head
    senderPath == receiverPath
  }

  def jvmIdFromActorRef(actorRef: ActorRef): String = remoteActorPath(actorRef).replace(".tcp","").replace(".udp","")

  /**
    * Hash arbitrary string
    */
  def hash(text: String): String = text

  /**
    * We don't care about the port, we just want the network hostname
    * @param actorRef
    * @return
    */
  def networkHostnameFromActorRef(actorRef: ActorRef): String = jvmIdFromActorRef(actorRef).split("//").head.split(":").head

  def runCommand(command: String): String = {
    s"$command" !!
  }
}
