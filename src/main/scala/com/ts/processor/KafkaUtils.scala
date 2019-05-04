package com.ts.processor

import play.api.libs.json.{JsError, JsSuccess, Json, Reads}

import scala.util.{Failure, Success, Try}

object KafkaUtils {

  def messageDeserializer[T](implicit reads: Reads[T]): (Array[Byte], Long) => Try[T] = {
    (bytes, offset) =>
      Try(Json.parse(bytes)).flatMap { parsed =>
        parsed.validate[T] match {
          case e: JsError =>
            Failure(ValidationError(offset, JsError.toJson(e).toString))
          case JsSuccess(v, _) => Success(v)
        }
      }
  }
}
