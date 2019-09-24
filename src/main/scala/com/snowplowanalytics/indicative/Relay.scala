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

import java.util.concurrent.TimeUnit

import cats.data.NonEmptyList
import cats.effect.{Clock, IO}
import scalaj.http._
import io.circe.Json

object Relay {

  val defaultIndicativeUri = "https://api.indicative.com/service/event"
  private val relayHeaders = NonEmptyList.of(
    ("Indicative-Client", "Snowplow-Relay-" + BuildInfo.version),
    ("Content-Type", "application/json; charset=utf-8")
  )

  def postSingleEvent(indicativeUri: String)(event: Json): IO[HttpResponse[String]] =
    IO {
      Http(indicativeUri)
        .postData(event.noSpaces)
        .headers(relayHeaders.head, relayHeaders.tail: _*)
        .asString
    }

  def postEventBatch(indicativeUri: String)(batchEvent: Json)(
    implicit c: Clock[IO]
  ): IO[(HttpResponse[String], Long)] =
    for {
      before <- c.monotonic(TimeUnit.MILLISECONDS)
      r <- IO {
        Http(indicativeUri + "/batch")
          .postData(batchEvent.noSpaces)
          .headers(relayHeaders.head, relayHeaders.tail: _*)
          .asString
      }
      after <- c.monotonic(TimeUnit.MILLISECONDS)
    } yield (r, after - before)

}
