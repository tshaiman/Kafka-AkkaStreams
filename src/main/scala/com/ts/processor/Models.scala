package com.ts.processor

import play.api.libs.json.{JsObject, JsResult, JsValue, Json, OFormat, Reads}

import scala.concurrent.Future

trait BaseResult

case class InfoMessage(id:String,data:String,start:Long,end:Long)
// The data type that will result from our validation
case class InfoResult(correlationId:String, status:Long,data:String) extends BaseResult
case class ErrorResult(reason:String) extends BaseResult

object InfoResult {
  def fromMessage(msg: InfoMessage): Either[String, InfoResult] = {
    Right(InfoResult(msg.id,msg.start + msg.end,msg.data))
  }
}

case class ValidationError(offset: Long, reason: String)
  extends Exception(s"Offset $offset failed validation: $reason")

// Some sort of storage mechanism
trait Database {
  def write(result: InfoResult): Future[Option[InfoResult]]
}