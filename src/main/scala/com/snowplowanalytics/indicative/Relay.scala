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
import cats.effect.IO

// circe
import io.circe.JsonObject

// hammock
import hammock._
import hammock.circe.implicits._
import hammock.jvm.Interpreter

object Relay {

  private implicit val interpreter = Interpreter[IO]
  private val indicativeUri        = uri"https://api.indicative.com/service/event"

  def postSingleEvent(event: JsonObject): IO[HttpResponse] =
    Hammock
      .request(Method.POST, indicativeUri, Map(), Some(event))
      .exec[IO]

  def postEventBatch(batchEvent: JsonObject): IO[HttpResponse] =
    Hammock
      .request(Method.POST, indicativeUri / "batch", Map(), Some(batchEvent))
      .exec[IO]

}
