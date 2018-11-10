package net.degols.filesgate.libs.cluster

import akka.actor.ActorRef
import org.joda.time.{DateTime, DateTimeZone}
import sys.process._

/**
  * Created by Gilles.Degols on 04-09-18.
  */
object Tools {
  def datetime(): DateTime = new DateTime().withZone(DateTimeZone.UTC)

  def difference(otherDatetime: DateTime): Long = otherDatetime.getMillis - datetime().getMillis

  /**
    * Return the full path to a given actor ref
    * @param actorRef
    */
  def remoteActorPath(actorRef: ActorRef): String = {
    var path = akka.serialization.Serialization.serializedActorPath(actorRef).split("#").head
    if(!path.contains("@")) {
      throw new Exception(s"Missing configuration to have a valid remote actor path for $actorRef.")
    }
    path
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
