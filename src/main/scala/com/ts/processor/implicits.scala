package com.ts.processor

import play.api.libs.json.{JsObject, JsResult, JsValue, Json, OFormat, Writes}

object MessageFormat extends OFormat[Message] {

  override def reads(json: JsValue): JsResult[Message] =
    for {
      id      <- (json \ "id").validate[String]
      data    <- (json \ "data").validate[String]
      start <- (json \ "start").validate[Long]
      end <- (json \ "end").validate[Long]
    } yield Message(id,data,start,end)


  override def writes(o: Message): JsObject = Json.obj(
      "id"        -> o.id,
      "data"      -> o.data,
      "start"     -> o.start,
      "end"     -> o.end
    )
}

object ResultFormat extends Writes[Result] {
  override def writes(o: Result): JsValue = Json.obj(
      "correlationId"  -> o.correlationId,
      "status" -> o.status,
      "data" -> o.data.toUpperCase()
  )
}
