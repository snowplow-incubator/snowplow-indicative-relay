/*
 * Copyright (c) 2018-2021 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration.{MILLISECONDS, NANOSECONDS}

import cats.Id
import cats.effect.Clock
import io.circe.Json

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.scalacheck.{IgluSchemas, JsonGenSchema}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import org.scalacheck.Gen

object Utils {
  implicit val idClock: Clock[Id] = new Clock[Id] {
    def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), MILLISECONDS)

    def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), NANOSECONDS)
  }

  def fetch(uri: String): (Gen[Json], Json) = {
    val schemaKey = SchemaKey
      .fromUri(uri)
      .getOrElse(throw new RuntimeException("Invalid Iglu URI"))

    val resolver = Resolver[Id](List(Registry.IgluCentral), None)

    val result = for {
      s <- IgluSchemas.lookup(resolver, schemaKey)
      a <- IgluSchemas.parseSchema(s)
    } yield (JsonGenSchema.json(a), s)

    result.fold(e => throw new RuntimeException(e), x => x)
  }

  def embedDataInContext(uri: String, data: String): String =
    s"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
      "data": [
        {
          "schema": "$uri",
          "data": $data
        }
      ]
    }"""

  def getTsvInput(input: List[(String, String)]): String =
    input.map(_._2).mkString("\t")

  def getTransformedSnowplowEvent(tsvInput: String): Event =
    (for {
      snowplowEvent <- Event.parse(tsvInput)
    } yield snowplowEvent).toEither.right.get

}
