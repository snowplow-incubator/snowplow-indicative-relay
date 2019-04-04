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

import java.util.concurrent.TimeUnit

import cats.effect.{Clock, IO}
import scalaj.http._
import io.circe.Json

object Relay {

  private val indicativeUri = "https://api.indicative.com/service/event"
  private val relayHeader   = "Indicative-Client" -> ("Snowplow-Relay-" + BuildInfo.version)

  def postSingleEvent(event: Json): IO[HttpResponse[String]] =
    IO {
      Http(indicativeUri)
        .postData(event.noSpaces)
        .header(relayHeader._1, relayHeader._2)
        .asString
    }

  def postEventBatch(batchEvent: Json)(
    implicit c: Clock[IO]
  ): IO[(HttpResponse[String], Long)] =
    for {
      before <- c.monotonic(TimeUnit.MILLISECONDS)
      r <- IO {
        Http(indicativeUri)
          .postData(batchEvent.noSpaces)
          .header(relayHeader._1, relayHeader._2)
          .asString
      }
      after <- c.monotonic(TimeUnit.MILLISECONDS)
    } yield (r, after - before)

}
