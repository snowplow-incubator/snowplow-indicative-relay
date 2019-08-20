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

import cats.syntax.either._

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.schemaddl.scalacheck.{IgluSchemas, JsonGenSchema}
import org.json4s.JValue
import org.scalacheck.Gen

object Utils {

  def fetch(uri: String): (Gen[JValue], JValue) = {
    val schemaKey = SchemaKey
      .fromUri(uri)
      .getOrElse(throw new RuntimeException("Invalid Iglu URI"))

    val result = for {
      s <- IgluSchemas.lookup(None)(schemaKey)
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

}
