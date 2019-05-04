package com.ts.processor

import play.api.libs.json.{JsObject, JsResult, JsValue, Json, OFormat, Writes}

object InfoMessageFormat extends OFormat[InfoMessage] {

  override def reads(json: JsValue): JsResult[InfoMessage] = {

    val result = for {
      id <- (json \ "id").validate[String]
      data <- (json \ "data").validate[String]
      start <- (json \ "start").validate[Long]
      end <- (json \ "end").validate[Long]
    } yield InfoMessage(id, data, start, end)
   result
  }


  override def writes(o: InfoMessage): JsObject = Json.obj(
      "id"        -> o.id,
      "data"      -> o.data,
      "start"     -> o.start,
      "end"     -> o.end
    )
}

object ResultFormat extends Writes[InfoResult] {
  override def writes(o: InfoResult): JsValue = Json.obj(
      "correlationId"  -> o.correlationId,
      "status" -> o.status,
      "data" -> o.data.toUpperCase()
  )
}
