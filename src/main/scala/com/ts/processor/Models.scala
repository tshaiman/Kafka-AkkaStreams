package com.ts.processor

import play.api.libs.json.{JsObject, JsResult, JsValue, Json, OFormat, Reads}

import scala.concurrent.Future

case class Message(id:String,data:String,start:Long,end:Long)
// The data type that will result from our validation
case class Result(correlationId:String, status:Long,data:String)

object Result {
  def fromMessage(msg: Message): Either[String, Result] = {
    Right(Result(msg.id,msg.start + msg.end,msg.data))
  }
}

// Some sort of storage mechanism
trait Database {
  def write(result: Result): Future[Unit]
}