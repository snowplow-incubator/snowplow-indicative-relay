/*
 * Copyright (c) 2018-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.indicative

// cats
import cats.data.EitherT
import cats.instances.either._
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.either._

// circe
import io.circe.syntax._
import io.circe.{Json, JsonObject}
import io.circe.parser.parse

// Analytics SDK
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem

/**
 * Contains functions for transforming Snowplow JSON into Indicative JSON. Outputs do not include the apiKey field.
 */
object Transformer {

  import FieldsExtraction._

  final case class TransformationError(message: String) extends Exception(message)

  /**
   * Transforms Snowplow enriched event string into Indicative event format
   * @param snowplowEvent Snowplow enriched event JSON string
   * @param inventory a set of inventory items returned by EventTransformer
   * @return either an error or a JsonObject containing Indicative event
   */
  def transform(
    snowplowEvent: String,
    inventory: Set[InventoryItem]
  ): Option[Either[TransformationError, JsonObject]] =
    (for {
      snowplowEvent <- EitherT
        .fromEither[Option](parse(snowplowEvent).leftMap(e => TransformationError(e.message)))
      indicativeEvent <- EitherT(snowplowJsonToIndicativeEvent(snowplowEvent, inventory))
    } yield indicativeEvent).value

  /**
   * Turns a Snowplow enriched event in a json format into a json ready do be consumed by Indicative.
   * @param snowplowJson Snowplow enriched event in a json format
   * @param inventory a set of inventory items returned by EventTransformer
   * @return None if the event doesn't contain a user identifying field (user_id,
   * client_session_user_id, or domain_userid). Otherwise, it returns either an event in Indicative
   * format or a transformation error.
   */
  def snowplowJsonToIndicativeEvent(snowplowJson: Json,
                                    inventory: Set[InventoryItem]): Option[Either[TransformationError, JsonObject]] = {
    val properties = flattenJson(snowplowJson, inventory)
    val eventName  = extractField(properties, "event_name")
    val userId = extractField(properties, "user_id")
      .leftFlatMap(_ => extractField(properties, "client_session_user_id"))
      .leftFlatMap(_ => extractField(properties, "domain_userid"))
      .toOption

    val eventTime = extractField(properties, "derived_tstamp").flatMap(timestampToMillis)

    userId.map { uid =>
      (eventName, properties.asRight[TransformationError], eventTime)
        .mapN { case (eName, props, eTime) => constructIndicativeJson(eName, uid, props, eTime) }
        .leftMap(decodingError => TransformationError(decodingError.message))
    }
  }

  private def constructIndicativeJson(eventName: String,
                                      uniqueId: String,
                                      properties: Map[String, Json],
                                      eventTime: Long): JsonObject =
    JsonObject(
      "eventName"     -> Json.fromString(eventName),
      "eventUniqueId" -> Json.fromString(uniqueId),
      "eventTime"     -> Json.fromLong(eventTime),
      "properties"    -> Json.fromFields(properties)
    )

  def constructBatchEvent(apiKey: String, events: List[JsonObject]): JsonObject =
    JsonObject(
      "apiKey" -> Json.fromString(apiKey),
      "events" -> Json.fromValues(events.map(_.asJson))
    )

}
