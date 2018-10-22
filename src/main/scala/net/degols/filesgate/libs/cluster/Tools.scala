package net.degols.filesgate.libs.cluster

import akka.actor.ActorRef
import org.joda.time.{DateTime, DateTimeZone}
import sys.process._

/**
  * Created by Gilles.Degols on 04-09-18.
  */
object Tools {
  def datetime(): DateTime = new DateTime().withZone(DateTimeZone.UTC)

  /**
    * Return the full path to a given actor ref
    * @param actorRef
    */
  def remoteActorPath(actorRef: ActorRef): String = akka.serialization.Serialization.serializedActorPath(actorRef).split("#").head

  def jvmIdFromActorRef(actorRef: ActorRef): String = remoteActorPath(actorRef).replace(".tcp","").replace(".udp","")

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
