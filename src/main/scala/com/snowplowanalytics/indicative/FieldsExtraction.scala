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

import java.time.ZonedDateTime

import scala.annotation.tailrec

import cats.syntax.either._
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.JsonShredder
import io.circe.Json

import Transformer.TransformationError

object FieldsExtraction {

  /**
   * Gets a json value out of the map and changes it to string, which is what the Indicative API expects
   */
  def extractField(properties: Map[String, Json], key: String): Either[TransformationError, String] =
    properties.get(key).flatMap(_.asString).toRight(TransformationError(s"Could not find the field with key $key"))

  /**
   * Converts a timestamp string with timezone into milliseconds
   */
  def timestampToMillis(timestampString: String): Either[TransformationError, Long] =
    Either
      .catchNonFatal(ZonedDateTime.parse(timestampString))
      .leftMap(error => TransformationError(s"Failed to parse timestamp $timestampString: ${error.getMessage}"))
      .map(_.toInstant.toEpochMilli)

  /**
   * Flattens Snowplow enriched events previously transformed by EventTransformer.
   * Brings all nested fields to top level. New keys are derived by combining the previous object key and
   * the nested value key. As Indicative does not support arrays, only the first element is taken, the rest is
   * discarded.
   * @param json json constructed from EventTransformer
   * @param inventory
   * @return a map where keys are derived from the input json
   */
  def flattenJson(json: Json, inventory: Set[Data.InventoryItem]): Map[String, Json] = {

    @tailrec
    def iterateObject(key: String, fields: List[(String, Json)], accumulator: Map[String, Json]): Map[String, Json] =
      fields match {
        case Nil =>
          accumulator
        case (headKey, headValue) :: tail =>
          val newAccumulator = iterate(simpleKeyName(key, headKey, inventory), headValue, accumulator)
          iterateObject(key, tail, newAccumulator)
      }

    def iterate(key: String, json: Json, accumulator: Map[String, Json]): Map[String, Json] =
      json.asArray
        .map(vector => if (vector.isEmpty) accumulator else iterate(key, vector.head, accumulator))
        .orElse(json.asObject.map(obj => iterateObject(key, obj.toList, accumulator)))
        .getOrElse(
          json.asNull
            .map(_ => accumulator)
            .getOrElse(accumulator.updated(key, json)))

    iterate("", json, Map.empty)
  }

  /**
   * Computes new key names. If `previousKey` corresponds to some entry
   * @param previousKey key carried from the top level
   * @param propertyName current key of the property
   * @param inventory inventory items returned by EventTransformer
   * @return new key name
   */
  private def simpleKeyName(previousKey: String, propertyName: String, inventory: Set[Data.InventoryItem]): String =
    inventory
      .find(item => Data.fixSchema(item.shredProperty, item.igluUri) == previousKey)
      .flatMap(item => getNameFromUri(item.igluUri))
      .map(toSnakeCase)
      .map(schemaName => s"${schemaName}_${propertyName}")
      .getOrElse(propertyName)

  private def getNameFromUri(uri: Data.IgluUri): Option[String] =
    JsonShredder.schemaPattern.findFirstMatchIn(uri).flatMap(m => Option(m.group(2)))

  private val regex                            = """(.)([\.\-]|(?=[A-Z]))(.)""".r
  private def toSnakeCase(str: String): String = regex.replaceAllIn(str, "$1_$3").toLowerCase

}
