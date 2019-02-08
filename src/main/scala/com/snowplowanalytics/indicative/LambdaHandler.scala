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

// Scala
import scala.collection.JavaConverters._
import scala.util.matching.Regex

// cats
import cats.data.EitherT
import cats.effect.IO
import cats.implicits._

// AWS
import com.amazonaws.services.lambda.runtime.events.KinesisEvent

// circe
import io.circe.JsonObject

// This library
import com.snowplowanalytics.indicative.Transformer.TransformationError
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer

class LambdaHandler {

  val apiKey: String =
    sys.env.getOrElse("INDICATIVE_API_KEY",
                      throw new RuntimeException("You must provide environment variable INDICATIVE_API_KEY"))
  val indicativeBatchSize = 100
  val appIdPattern: Regex = sys.env.getOrElse("APP_ID_REGEX", ".*").r

  def recordHandler(event: KinesisEvent): Unit = {
    val events: List[Either[TransformationError, JsonObject]] = event.getRecords.asScala
      .map { record =>
        val kinesisDataArray: Either[TransformationError, String] = Option(record.getKinesis)
          .flatMap(underlying => Option(underlying.getData))
          .filter(_.hasArray)
          .map(data => new String(data.array, "UTF-8"))
          .toRight(TransformationError("Could not retrieve underlying Kinesis Record"))

        (for {
          dataArray <- EitherT.fromEither[Option](kinesisDataArray)
          snowplowEvent <- EitherT.fromEither[Option](
            EventTransformer
              .transformWithInventory(dataArray)
              .leftMap(errors => TransformationError(errors.mkString("\n  * "))))
          indicativeEvent <- EitherT(Transformer.transform(snowplowEvent.event, snowplowEvent.inventory, appIdPattern))
        } yield indicativeEvent).value
      }
      .toList
      .flatten

    val (errors, jsons) = events.separate

    val sendEvents = jsons
      .sliding(indicativeBatchSize, indicativeBatchSize)
      .toList
      .map(js => Relay.postEventBatch(Transformer.constructBatchEvent(apiKey, jsons)))
      .sequence

    sendEvents
      .productL(IO(errors.foreach(error => println("[Json transformation error]: " + error))))
      .flatTap(responses =>
        IO(responses.filter(_.status.code != 200).foreach(r => println("[HTTP error]: " + r.entity.content))))
      .unsafeRunSync()
  }

}
