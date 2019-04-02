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

import scala.collection.JavaConverters._

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.implicits._
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer
import io.circe.JsonObject

import Transformer.TransformationError

class LambdaHandler {

  val apiKey: String =
    sys.env.getOrElse("INDICATIVE_API_KEY",
                      throw new RuntimeException("You must provide environment variable INDICATIVE_API_KEY"))
  val indicativeBatchSize = 100
  implicit val c: Clock[IO] = Clock.create[IO]

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
          indicativeEvent <- EitherT(Transformer.transform(snowplowEvent.event, snowplowEvent.inventory))
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
      .flatTap { responses =>
        IO {
          responses.foreach(r => println(s"I spent ${r._2} ms sending events to Indicative"))
        } >>
        IO {
          responses
          .filter(_._1.status.code != 200)
          .foreach(r => println("[HTTP error]: " + r._1.entity.content))
        }
      }
      .unsafeRunSync()
  }

}
