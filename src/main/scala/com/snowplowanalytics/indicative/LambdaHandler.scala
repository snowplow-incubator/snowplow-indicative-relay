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

import scala.collection.JavaConverters._

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.implicits._
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer
import io.circe.Json

import Transformer.TransformationError

class LambdaHandler {
  import LambdaHandler._

  val apiKey: String = getConfig("INDICATIVE_API_KEY", required = true, "")
  val unusedEvents
    : List[String] = getConfig("UNUSED_EVENTS", required = false, Filters.unusedEvents).split(",").toList // eg, `UNUSED_EVENTS=page_ping,app_heartbeat`
  val unusedAtomicFields: List[String] = getConfig("UNUSED_ATOMIC_FIELDS", required = false, Filters.unusedAtomicFields)
    .split(",")
    .toList // eg, `UNUSED_ATOMIC_FIELDS=etl_tstamp,geo_longitude`
  val unusedContexts: List[String] = getConfig("UNUSED_CONTEXTS", required = false, Filters.unusedContexts)
    .split(",")
    .toList // eg, `UNUSED_CONTEXTS=performance_timing,geolocation_context`

  // Number of events in a payload sent to Indicative is limited to 100
  val indicativeBatchSize = 100
  // Size of a payload sent to Indicative is limited to 1Mb
  val indicativePayloadBytesSize = 1000000
  implicit val c: Clock[IO]      = Clock.create[IO]

  def recordHandler(event: KinesisEvent): Unit = {
    val events: List[Either[TransformationError, Json]] = event.getRecords.asScala
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
          indicativeEvent <- EitherT(Transformer
            .transform(snowplowEvent.event, snowplowEvent.inventory, unusedEvents, unusedAtomicFields, unusedContexts))
        } yield indicativeEvent).value
      }
      .toList
      .flatten

    val (errors, jsons) = events.separate

    val (toSend, tooBig) = Transformer
      .constructBatchesOfEvents(apiKey, jsons, indicativeBatchSize, indicativePayloadBytesSize)

    val tooBigDebugging = IO {
      tooBig.foreach { e =>
        println(s"Event was too big to send to Indicative, it exceeds the 1Mb limit: $e")
      }
    }

    val sendEvents = toSend
      .map(js => Relay.postEventBatch(js))
      .sequence

    (tooBigDebugging >>
      sendEvents
        .productL(IO(errors.foreach(error => println("[Json transformation error]: " + error))))
        .flatTap { responses =>
          IO {
            responses.foreach(r => println(s"I spent ${r._2} ms sending events to Indicative"))
          } >>
            IO {
              responses
                .filter(_._1.code != 200)
                .foreach(r => println(s"[HTTP error]: ${r._1.body}"))
            }
        }).unsafeRunSync()
    ()
  }

}

object LambdaHandler {

  /** Gets the config value from Lambda's environment variables or from defaults.
   * Short-circuiting on required env var.
   */
  def getConfig(envVar: String, required: Boolean, default: String): String =
    if (required)
      sys.env.getOrElse(envVar, throw new RuntimeException(s"You must provide environment variable $envVar."))
    else sys.env.getOrElse(envVar, default)

}
