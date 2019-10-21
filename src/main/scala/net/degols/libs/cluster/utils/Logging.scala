package net.degols.libs.cluster.utils

import org.slf4j.LoggerFactory

/**
  * Provide default logger to any class, and remove the overhead of calling debug often even if the log debug
  * is not activated.
  * The small condition each time saves around 7µs. Worth it if we call the logger for every message. Behind the debug
  * itself, they also do the condition, but for whatever reason, it is not as efficient.
  *
  * For efficiency, we also have small methods to force the JIT inlining (default value is 35 bytes)
  */
trait Logging {
  final protected lazy val l = LoggerFactory.getLogger(getClass)

  final protected def trace(m: => String): Unit = if(l.isTraceEnabled)l.trace(m)

  final protected def trace(m: => String, o: => Object): Unit = if(l.isTraceEnabled)l.trace(m,o)

  final protected def debug(m: => String): Unit = if(l.isDebugEnabled)l.debug(m)

  final protected def debug(m: => String, o: => Object): Unit = if(l.isDebugEnabled)l.debug(m,o)

  final protected def info(m: => String): Unit = if(l.isInfoEnabled)l.info(m)

  final protected def info(m: => String, o: => Object): Unit = if(l.isInfoEnabled)l.info(m,o)

  final protected def warn(m: => String): Unit = if(l.isWarnEnabled)l.warn(m)

  final protected def warn(m: => String, o: => Object): Unit = if(l.isWarnEnabled)l.warn(m,o)

  final protected def error(m: => String): Unit = if(l.isErrorEnabled)l.error(m)

  final protected def error(m: => String, o: => Object): Unit = if(l.isErrorEnabled)l.error(m,o)
}
