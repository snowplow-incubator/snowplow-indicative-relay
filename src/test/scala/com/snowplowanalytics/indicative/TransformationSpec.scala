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

import com.snowplowanalytics.indicative.Transformer._
import com.snowplowanalytics.indicative.Utils._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import org.scalacheck.Prop.forAll
import org.specs2.ScalaCheck
import org.specs2.execute.Result
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification

class TransformationSpec extends Specification with ScalaCheck with Matchers {
  "flattenJson" >> {
    "should work on empty arrays" >> {
      val input = json"""
      {
        "foo": []
      }
      """

      FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
    }

    "should work on empty objects" >> {
      val input = json"""
      {
        "foo": {}
      }
      """

      FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
    }

    "should not parse null values as \"null\"" >> {
      val input = json"""
      {
        "foo": null
      }
      """

      FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
    }
  }

  "getUserId" >> {

    "should return user_id if that is available" >> {
      val tsvInput = getTsvInput(Instances.Web.input)
      val event =
        (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
          parsed).right.get
      val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
      val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
      val expected       = Option(Expectations.userId)

      getUserId(flattenedEvent) shouldEqual expected
    }

    "should return client_session_userId if that is available and there is no user_id" >> {
      val input = Instances.Mobile.input.map {
        case (key, _) if key == "user_id" => (key, "")
        case (key, value)                 => (key, value)
      }
      val tsvInput = getTsvInput(input)
      val event =
        (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
          parsed).right.get
      val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
      val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
      val expected       = Option(Expectations.clientSessionUserId)

      getUserId(flattenedEvent) shouldEqual expected
    }
    "should return domain_userid if that is available and there is no user_id or client_session_userId" >> {
      val input = Instances.Web.input.map {
        case (key, _) if key == "user_id" => (key, "")
        case (key, value)                 => (key, value)
      }
      val tsvInput = getTsvInput(input)
      val event =
        (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
          parsed).right.get
      val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
      val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
      val expected       = Option(Expectations.domainUserid)

      getUserId(flattenedEvent) shouldEqual expected
    }
    "should return None if there is no user_id, client_session_userId or domain_userid" >> {
      val input = Instances.Web.input.map {
        case (key, _) if key == "user_id"       => (key, "")
        case (key, _) if key == "domain_userid" => (key, "")
        case (key, value)                       => (key, value)
      }
      val tsvInput = getTsvInput(input)
      val event =
        (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
          parsed).right.get
      val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
      val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)

      getUserId(flattenedEvent) shouldEqual None
    }
  }

  "getEventName" >> {
    "with struct event" >> {
      "in the default case should return se_action if that is available" >> {
        val tsvInput = getTsvInput(Instances.Web.structInput)
        val event =
          (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
            parsed).right.get
        val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
        val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
        val expected       = Option(Expectations.seAction)

        getEventName(flattenedEvent, Relay.defaultStructuredEventName).toOption shouldEqual expected
      }

      "in the default case should return event_name if that is available and there is no se_action" >> {
        val tsvInput = getTsvInput(Instances.Web.structInput.map {
          case (key, _) if key == "se_action" => (key, "")
          case (key, value)                   => (key, value)
        })
        val event =
          (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
            parsed).right.get
        val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
        val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
        val expected       = Option(Expectations.structEventName)

        getEventName(flattenedEvent, Relay.defaultStructuredEventName).toOption shouldEqual expected
      }

      "should return the value of the field that was specified" >> {
        val tsvInput = getTsvInput(Instances.Web.structInput)
        val event =
          (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
            parsed).right.get
        val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
        val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
        val expected       = Option(Expectations.seCategory)

        getEventName(flattenedEvent, "se_category").toOption shouldEqual expected
      }
    }

    "with event other than struct" >> {
      "should return event_name in the default case" >> {
        val tsvInput = getTsvInput(Instances.Web.input)
        val event =
          (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
            parsed).right.get
        val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
        val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
        val expected       = Option(Expectations.eventName)

        getEventName(flattenedEvent, Relay.defaultStructuredEventName).toOption shouldEqual expected
      }

      "should return event_name even when a struct event field to be used is specified" >> {
        val tsvInput = getTsvInput(Instances.Web.input)
        val event =
          (for { parsed <- io.circe.parser.parse(getTransformedSnowplowEvent(tsvInput).toJson(true).noSpaces) } yield
            parsed).right.get
        val inventory      = getTransformedSnowplowEvent(tsvInput).inventory
        val flattenedEvent = FieldsExtraction.flattenJson(event, inventory)
        val expected       = Option(Expectations.eventName)

        getEventName(flattenedEvent, "se_category").toOption shouldEqual expected
      }
    }
  }

  "constructBatchesOfEvents" >> {
    "should return events as too big" >> {
      val base = "a" -> Json.fromString(List.fill(20)("a").mkString)
      val js   = List(Json.obj(base))
      val (toSend, tooBig) =
        constructBatches[Json](getSize, constructJson("a"), js, 10, 10)

      toSend shouldEqual Nil
      tooBig shouldEqual js
    }

    "should correctly batch events according to the specified size" >> {
      val base = "a" -> Json.fromString("a")
      val js   = List.fill(12)(Json.obj(base))
      val (toSend, tooBig) =
        constructBatches(getSize, constructJson("a"), js, 10, 1000)
      val expected = List(
        Json.obj(
          "apiKey" -> Json.fromString("a"),
          "events" -> Json.fromValues(List.fill(2)(Json.obj(base)))
        ),
        Json.obj(
          "apiKey" -> Json.fromString("a"),
          "events" -> Json.fromValues(List.fill(10)(Json.obj(base)))
        )
      )

      toSend shouldEqual expected
      tooBig shouldEqual Nil
    }

    "should correctly batch events according to the specified payload size" >> {
      val base = "a" -> Json.fromString(List.fill(20)("a").mkString)
      val size = Json.obj(base).noSpaces.getBytes("utf-8").length
      val js   = List.fill(20)(Json.obj(base))
      val (toSend, tooBig) =
        constructBatches(getSize, constructJson("a"), js, 10, size * 5)
      val elem = Json.obj(
        "apiKey" -> Json.fromString("a"),
        "events" -> Json.fromValues(List.fill(4)(Json.obj(base)))
      )

      toSend shouldEqual List(elem, elem, elem, elem, elem)
      tooBig shouldEqual Nil
    }
  }

  "integration tests" >> {
    def runTest(uri: String) = {
      val (gen, _) = fetch(uri)

      val inputGen = gen.map { json =>
        Instances.Web.input
          .map {
            case (key, value) =>
              if (key == "contexts") (key, embedDataInContext(uri, json.noSpaces))
              else (key, value)
          }
          .map(_._2)
          .mkString("\t")
      }

      forAll(inputGen) { str =>
        val result = getTransformationResult(str, Instances.Options.transformationOptions)

        result must beSome and result.map(_ must beRight).get
      }
    }

    def getTransformationResult(event: String,
                                options: TransformationOptions): Option[Either[TransformationError, Json]] =
      (for {
        snowplowEvent <- EitherT.fromEither[Option](
          Event
            .parse(event)
            .toEither
            .leftMap(error => TransformationError(error.toString)))
        indicativeEvent <- EitherT(
          Transformer
            .transform(snowplowEvent.toJson(true).noSpaces, snowplowEvent.inventory, options))
      } yield indicativeEvent).value

    "should be transformed with real contexts generated from schemas" >> {
      val uris = List(
        "iglu:com.getvero/delivered/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow.enrichments/weather_enrichment_config/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/ad_conversion/jsonschema/1-0-0"
      )

      Result.foreach(uris)(runTest)
    }

    "no filters" >> {
      val expected         = Expectations.unfilteredIndicativeEvent
      val tsvInput: String = getTsvInput(Instances.Web.input)
      val options          = Instances.Options.transformationOptions
      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual Some(Right(expected))
    }

    "filter out unused events" >> {
      val expected                   = None
      val tsvInput: String           = getTsvInput(Instances.Web.input)
      val unusedEvents: List[String] = Instances.Filters.unusedEvents.split(",").toList
      val options                    = Instances.Options.transformationOptions.copy(unusedEvents = unusedEvents)

      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual expected
    }

    "filter out unused atomic fields" >> {
      val expected                         = Expectations.indicativeEventWithTrimmedAtomicFields
      val tsvInput: String                 = getTsvInput(Instances.Web.input)
      val unusedAtomicFields: List[String] = Instances.Filters.unusedAtomicFields.split(",").toList
      val options =
        Instances.Options.transformationOptions.copy(unusedAtomicFields = unusedAtomicFields)
      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual Some(Right(expected))
    }

    "filter out unused contexts" >> {
      val expected                     = Expectations.indicativeEventWithTrimmedContextFields
      val tsvInput: String             = getTsvInput(Instances.Web.input)
      val unusedContexts: List[String] = Instances.Filters.unusedContexts.split(",").toList
      val options                      = Instances.Options.transformationOptions.copy(unusedContexts = unusedContexts)
      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual Some(Right(expected))
    }

    "filter out events without a user identifying property" >> {
      val expected = None
      val tsvInput = getTsvInput(
        Instances.Web.input
          .map {
            case (fieldName, _) if List("user_id", "domain_userid").contains(fieldName) => fieldName -> ""
            case a                                                                      => a
          })
      val options = Instances.Options.transformationOptions
      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual expected
    }

    "mobile events" >> {
      val expected = Expectations.mobileIndicativeEvent
      val event = Instances.Mobile.input
        .map {
          case (fieldName, _) if List("user_id", "domain_userid").contains(fieldName) => fieldName -> ""
          case a                                                                      => a
        }
        .map(_._2)
        .mkString("\t")
      val options = Instances.Options.transformationOptions
      val result =
        getTransformationResult(event, options)

      result shouldEqual Some(Right(expected))
    }

    "struct events using se_action" >> {
      val expected                     = Expectations.indicativeEventUsingStructuredAction
      val tsvInput: String             = getTsvInput(Instances.Web.structInput)
      val unusedContexts: List[String] = Instances.Filters.unusedContexts.split(",").toList
      val options                      = Instances.Options.transformationOptions.copy(unusedContexts = unusedContexts)
      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual Some(Right(expected))
    }

    "struct events using se_category" >> {
      val expected                       = Expectations.indicativeEventUsingStructuredCategory
      val tsvInput: String               = getTsvInput(Instances.Web.structInput)
      val unusedContexts: List[String]   = Instances.Filters.unusedContexts.split(",").toList
      val structedEventNameField: String = "se_category"
      val options = Instances.Options.transformationOptions.copy(unusedContexts = unusedContexts,
                                                                 structuredEventNameField = structedEventNameField)
      val result =
        getTransformationResult(tsvInput, options)

      result shouldEqual Some(Right(expected))
    }
  }
}
