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

import cats.data.EitherT
import cats.instances.option._
import cats.syntax.either._
import io.circe._
import io.circe.literal._
import org.json4s.jackson.JsonMethods._

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer
import com.snowplowanalytics.indicative.Transformer.TransformationError

import org.specs2.{ScalaCheck, Specification}
import org.scalacheck.Prop.forAll
import org.specs2.execute.Result
import org.specs2.matcher.Matchers

class TransformationSpec extends Specification with ScalaCheck with Matchers {
  def is = s2"""
    integration test (no filters)                                     $e1
    integration test (filter out unused events)                       $e2
    integration test (filter out unused atomic fields)                $e3
    integration test (filter out unused contexts)                     $e4
    integration test without user identifying property                $e5
    should be transformed with real contexts generated from schemas   $e6
    flattenJson should work on empty arrays                           $e7
    flattenJson should work on empty objects                          $e8
    constructBatchesOfEvents return events as too big                 $e9
    constructBatchesOfEvents return batches of the specified size     $e10
    constructBatchesOfEvents return batches according to payload size $e11
  """

  val apiKey = "API_KEY"

  def runTest(uri: String) = {
    val (gen, _) = Utils.fetch(uri)

    val inputGen = gen.map { json =>
      Instances.input
        .map {
          case (key, value) =>
            if (key == "contexts") (key, Utils.embedDataInContext(uri, compact(json)))
            else (key, value)
        }
        .unzip
        ._2
        .mkString("\t")
    }

    forAll(inputGen) { str =>
      val result = getTransformationResult(str,
                                           Instances.emptyFilter.split(",").toList,
                                           Instances.emptyFilter.split(",").toList,
                                           Instances.emptyFilter.split(",").toList)

      result must beSome and result.map(_ must beRight).get
    }
  }

  def getTransformationResult(event: String,
                              unusedEvents: List[String],
                              unusedAtomicFields: List[String],
                              unusedContexts: List[String]): Option[Either[TransformationError, Json]] =
    (for {
      snowplowEvent <- EitherT.fromEither[Option](
        EventTransformer
          .transformWithInventory(event)
          .leftMap(errors => TransformationError(errors.mkString("\n  * "))))
      indicativeEvent <- EitherT(
        Transformer
          .transform(snowplowEvent.event, snowplowEvent.inventory, unusedEvents, unusedAtomicFields, unusedContexts))
    } yield indicativeEvent).value

  def e1 = {
    val expected                         = Expectations.unfilteredIndicativeEvent
    val unusedEvents: List[String]       = Instances.emptyFilter.split(",").toList
    val unusedAtomicFields: List[String] = Instances.emptyFilter.split(",").toList
    val unusedContexts: List[String]     = Instances.emptyFilter.split(",").toList
    val result                           = getTransformationResult(Instances.tsvInput, unusedEvents, unusedAtomicFields, unusedContexts)

    result shouldEqual Some(Right(expected))
  }

  def e2 = {
    val unusedEvents: List[String]       = Instances.unusedEvents.split(",").toList
    val unusedAtomicFields: List[String] = Instances.emptyFilter.split(",").toList
    val unusedContexts: List[String]     = Instances.emptyFilter.split(",").toList
    val result                           = getTransformationResult(Instances.tsvInput, unusedEvents, unusedAtomicFields, unusedContexts)

    result must_== None
  }

  def e3 = {
    val expected                         = Expectations.indicativeEventWithTrimmedAtomicFields
    val unusedEvents: List[String]       = Instances.emptyFilter.split(",").toList
    val unusedAtomicFields: List[String] = Instances.unusedAtomicFields.split(",").toList
    val unusedContexts: List[String]     = Instances.emptyFilter.split(",").toList
    val result                           = getTransformationResult(Instances.tsvInput, unusedEvents, unusedAtomicFields, unusedContexts)

    result shouldEqual Some(Right(expected))
  }

  def e4 = {
    val expected                         = Expectations.indicativeEventWithTrimmedContextFields
    val unusedEvents: List[String]       = Instances.emptyFilter.split(",").toList
    val unusedAtomicFields: List[String] = Instances.emptyFilter.split(",").toList
    val unusedContexts: List[String]     = Instances.unusedContexts.split(",").toList
    val result                           = getTransformationResult(Instances.tsvInput, unusedEvents, unusedAtomicFields, unusedContexts)

    result shouldEqual Some(Right(expected))
  }

  def e5 = {
    val event = Instances.input
      .map {
        case (fieldName, _) if List("user_id", "domain_userid").contains(fieldName) => fieldName -> ""
        case a                                                                      => a
      }
      .unzip
      ._2
      .mkString("\t")
    val unusedEvents: List[String]       = Instances.emptyFilter.split(",").toList
    val unusedAtomicFields: List[String] = Instances.emptyFilter.split(",").toList
    val unusedContexts: List[String]     = Instances.emptyFilter.split(",").toList
    val result                           = getTransformationResult(event, unusedEvents, unusedAtomicFields, unusedContexts)

    result must_== None
  }

  def e6 = {
    val uris = List(
      "iglu:com.getvero/delivered/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.snowplow.enrichments/weather_enrichment_config/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.snowplow/ad_conversion/jsonschema/1-0-0"
    )

    Result.foreach(uris)(runTest)
  }

  def e7 = {
    val input = json"""
      {
        "foo": []
      }
    """

    FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
  }

  def e8 = {
    val input = json"""
      {
        "foo": {}
      }
    """

    FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
  }

  def e9 = {
    val base = "a" -> Json.fromString(List.fill(20)("a").mkString)
    val js   = List(Json.obj(base))
    val (toSend, tooBig) =
      Transformer.constructBatches(Transformer.getSize _, Transformer.constructJson("a") _, js, 10, 10)
    toSend shouldEqual Nil
    tooBig shouldEqual js
  }

  def e10 = {
    val base = "a" -> Json.fromString("a")
    val js   = List.fill(12)(Json.obj(base))
    val (toSend, tooBig) =
      Transformer.constructBatches(Transformer.getSize _, Transformer.constructJson("a") _, js, 10, 1000)
    toSend shouldEqual List(
      JsonObject(
        "apiKey" -> Json.fromString("a"),
        "events" -> Json.fromValues(List.fill(10)(Json.obj(base)))
      ),
      JsonObject(
        "apiKey" -> Json.fromString("a"),
        "events" -> Json.fromValues(List.fill(2)(Json.obj(base)))
      )
    )
    tooBig shouldEqual Nil
  }

  def e11 = {
    val base = "a" -> Json.fromString(List.fill(20)("a").mkString)
    val size = Json.obj(base).noSpaces.getBytes("utf-8").length
    val js   = List.fill(20)(Json.obj(base))
    val (toSend, tooBig) =
      Transformer.constructBatches(Transformer.getSize _, Transformer.constructJson("a") _, js, 10, size * 5)
    val elem = JsonObject(
      "apiKey" -> Json.fromString("a"),
      "events" -> Json.fromValues(List.fill(5)(Json.obj(base)))
    )
    toSend shouldEqual List(elem, elem, elem, elem)
    tooBig shouldEqual Nil
  }

}
