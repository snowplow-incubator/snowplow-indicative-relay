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

// cats
import cats.effect.IO
import cats.implicits._

// AWS
import com.amazonaws.services.lambda.runtime.events.KinesisEvent

// This library
import com.snowplowanalytics.indicative.Transformer.TransformationError
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer

class LambdaHandler {

  val apiKey: String =
    sys.env.getOrElse("INDICATIVE_API_KEY",
                      throw new RuntimeException("You must provide environment variable INDICATIVE_API_KEY"))

  def recordHandler(event: KinesisEvent): Unit = {
    val events = event.getRecords.asScala.map { record =>
      Option(record.getKinesis)
        .flatMap(underlying => Option(underlying.getData))
        .filter(_.hasArray)
        .map(data => new String(data.array, "UTF-8"))
        .toRight(TransformationError("Could not retrieve underlying Kinesis Record"))
        .flatMap(line =>
          EventTransformer
            .transformWithInventory(line)
            .leftMap(errors => TransformationError(errors.mkString("\n  * "))))
        .flatMap(eventWithInventory => Transformer.transform(eventWithInventory.event, eventWithInventory.inventory))
    }.toList

    val (errors, jsons) = events.separate

    Relay
      .postEventBatch(Transformer.constructBatchEvent(apiKey, jsons))
      .productL(IO(errors.foreach(error => println("[Json transformation error]: " + error))))
      .flatTap(response => IO(if (response.status.code != 200) println("[HTTP error]: " + response.entity.content)))
      .unsafeRunSync // Lambda logs exceptions nicely
  }

}
