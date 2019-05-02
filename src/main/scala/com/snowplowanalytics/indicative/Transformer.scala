/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data.EitherT
import cats.instances.either._
import cats.instances.option._
import cats.syntax.apply._
import cats.syntax.either._
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem
import io.circe.Json
import io.circe.parser.parse

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
    inventory: Set[InventoryItem],
    unusedEvents: List[String],
    unusedAtomicFields: List[String],
    unusedContexts: List[String]
  ): Option[Either[TransformationError, Json]] =
    (for {
      snowplowEvent <- EitherT
        .fromEither[Option](parse(snowplowEvent).leftMap(e => TransformationError(e.message)))
      indicativeEvent <- EitherT(
        snowplowJsonToIndicativeEvent(snowplowEvent, inventory, unusedEvents, unusedAtomicFields, unusedContexts))
    } yield indicativeEvent).value

  /**
   * Turns a Snowplow enriched event in a json format into a json ready do be consumed by Indicative.
   * @param snowplowJson Snowplow enriched event in a json format
   * @param inventory a set of inventory items returned by EventTransformer
   * @return None if the event is in the unusedEvents list, or if it doesn't contain a user identifying field (user_id,
   * client_session_user_id, or domain_userid). Otherwise, it returns either an event in Indicative
   * format or a transformation error.
   */
  def snowplowJsonToIndicativeEvent(snowplowJson: Json,
                                    inventory: Set[InventoryItem],
                                    unusedEvents: List[String],
                                    unusedAtomicFields: List[String],
                                    unusedContexts: List[String]): Option[Either[TransformationError, Json]] = {
    val flattenedEvent      = flattenJson(snowplowJson, inventory)
    val unusedContextFields =
      // unusedContexts can never be empty because the env var must be set.
      // But it can contain a single empty string, if no contexts are to be filtered out.
      if (unusedContexts.head == "") List()
      else
        for {
          context <- unusedContexts
          keys    <- flattenedEvent.filterKeys(_.startsWith(context)).keySet
        } yield keys
    val properties = flattenedEvent -- unusedAtomicFields -- unusedContextFields
    val eventName  = extractField(flattenedEvent, "event_name")
    val userId =
      if (eventName.isRight && unusedEvents.contains(eventName.right.toOption.get)) None // Filter out unused events
      else
        extractField(properties, "user_id")
          .leftFlatMap(_ => extractField(properties, "client_session_user_id"))
          .leftFlatMap(_ => extractField(properties, "domain_userid"))
          .toOption

    val eventTime = extractField(flattenedEvent, "derived_tstamp").flatMap(timestampToMillis)

    userId.map { uid =>
      (eventName, properties.asRight[TransformationError], eventTime)
        .mapN { case (eName, props, eTime) => constructIndicativeJson(eName, uid, props, eTime) }
        .leftMap(decodingError => TransformationError(decodingError.message))
    }
  }

  private def constructIndicativeJson(eventName: String,
                                      uniqueId: String,
                                      properties: Map[String, Json],
                                      eventTime: Long): Json =
    Json.obj(
      "eventName"     -> Json.fromString(eventName),
      "eventUniqueId" -> Json.fromString(uniqueId),
      "eventTime"     -> Json.fromLong(eventTime),
      "properties"    -> Json.fromFields(properties)
    )

  /**
   * Construct batches of events which respect both the batch size limit (100) and the payload size
   * limit (1Mb).
   * @param apiKey the api key to fill the jsons with
   * @param events the events to form batches from
   * @param maxBatchSize maxmimum number of elements which can be in a batch
   * @param maxBytesSize maximum size in bytes of a payload
   * @return (a series of payloads ready to be sent, events that were too big to be sent (>=1Mb on
   * their own))
   */
  def constructBatchesOfEvents(
    apiKey: String,
    events: List[Json],
    maxBatchSize: Int,
    maxBytesSize: Int
  ): (List[Json], List[Json]) = {
    @scala.annotation.tailrec
    def go(
      count: Int,
      size: Int,
      originalL: List[Json],
      tmpL: List[Json],
      newL: List[Json],
      tooBigL: List[Json]
    ): (List[Json], List[Json]) =
      (originalL, tmpL) match {
        case (Nil, Nil)       => (newL, tooBigL)
        case (Nil, remainder) => (constructBatch(apiKey, remainder) :: newL, tooBigL)
        // we remove unique events that do not fit on their own
        case (h :: t, _) if getSize(h) >= maxBytesSize =>
          go(count, size, t, tmpL, newL, h :: tooBigL)
        case (h :: _, tmp) if (count + 1) > maxBatchSize || (size + getSize(h)) >= maxBytesSize =>
          go(0, 0, originalL, Nil, constructBatch(apiKey, tmp) :: newL, tooBigL)
        case (h :: t, tmp) =>
          go(count + 1, size + getSize(h), t, h :: tmp, newL, tooBigL)
      }

    go(0, 0, events, Nil, Nil, Nil)
  }

  private def getSize(json: Json): Int =
    json.noSpaces.getBytes("utf-8").length

  def constructBatch(apiKey: String, events: List[Json]): Json =
    Json.obj(
      "apiKey" -> Json.fromString(apiKey),
      "events" -> Json.fromValues(events)
    )

}
