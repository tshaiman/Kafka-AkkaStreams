package com.ts.processor

import scala.concurrent.{ ExecutionContext, Future }

class FakeDatabase extends Database {
  implicit val ex: ExecutionContext = ExecutionContext.global

  override def write(result: InfoResult): Future[Option[InfoResult]] = Future {
    Thread.sleep(40)
    println(s"writing result to db. ResultId = ${result.correlationId}")
    Some(result)
  }
}
