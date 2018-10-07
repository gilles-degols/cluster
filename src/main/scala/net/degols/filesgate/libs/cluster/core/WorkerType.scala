package net.degols.filesgate.libs.cluster.core

/**
  * A WorkerType manages all worker instances of the same type as itself.
  * @param id
  */
class WorkerType(val id: String) {
  private var _workers: List[Worker] = List.empty[Worker]

  def addWorker(rawWorker: Worker): Worker = {
    _workers.find(_ == rawWorker) match {
      case Some(previousWorker) => previousWorker
      case None =>
        _workers = _workers :+ rawWorker
        rawWorker
    }
  }

  def workers: List[Worker] = _workers

  def canEqual(a: Any): Boolean = a.isInstanceOf[WorkerType]
  override def equals(that: Any): Boolean =
    that match {
      case that: WorkerType => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = s"WorkerType:$id".hashCode
}
