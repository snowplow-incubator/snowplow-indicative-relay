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

import cats.data.EitherT
import cats.instances.option._
import cats.syntax.either._
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

    integration test                                                $e1
    integration test without user identifying property              $e2
    should be transformed with real contexts generated from schemas $e3
    flattenJson should work on empty arrays                         $e4
    flattenJson should work on empty objects                        $e5

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
      val result = (for {
        snowplowEvent <- EitherT.fromEither[Option](
          EventTransformer
            .transformWithInventory(str)
            .leftMap(errors => TransformationError(errors.mkString("\n  * "))))
        indicativeEvent <- EitherT(Transformer.transform(snowplowEvent.event, snowplowEvent.inventory))
      } yield indicativeEvent).value

      result must beSome and (result.map(_ must beRight).get)
    }
  }

  def e1 = {

    val expected = json"""
      {
        "eventName": "link_click_test",
        "eventUniqueId": "jon.doe@email.com",
        "eventTime": 1532217837886,
        "properties": {
           "page_urlhost" : "www.snowplowanalytics.com",
           "br_features_realplayer" : null,
           "etl_tstamp" : "2018-07-20T00:01:25.292Z",
           "web_page_inLanguage" : "en-US",
           "dvce_ismobile" : null,
           "geo_latitude" : 37.443604,
           "performance_timing_domContentLoadedEventEnd" : 1415358091309,
           "refr_medium" : null,
           "performance_timing_domainLookupStart" : 1415358090102,
           "ti_orderid" : null,
           "br_version" : null,
           "base_currency" : null,
           "v_collector" : "clj-tomcat-0.1.0",
           "mkt_content" : null,
           "collector_tstamp" : "2018-07-20T00:02:05Z",
           "os_family" : null,
           "performance_timing_unloadEventEnd" : 1415358090287,
           "ti_sku" : null,
           "event_vendor" : "com.snowplowanalytics.snowplow",
           "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
           "performance_timing_requestStart" : 1415358090183,
           "br_renderengine" : null,
           "br_lang" : null,
           "tr_affiliation" : null,
           "ti_quantity" : null,
           "ti_currency" : null,
           "geo_country" : "US",
           "user_fingerprint" : "2161814971",
           "performance_timing_responseStart" : 1415358090265,
           "mkt_medium" : null,
           "page_urlscheme" : "http",
           "ua_parser_context_osMinor" : null,
           "ti_category" : null,
           "ua_parser_context_useragentFamily" : "IE",
           "pp_yoffset_min" : null,
           "br_features_quicktime" : null,
           "event" : "page_view",
           "refr_urlhost" : null,
           "user_ipaddress" : "92.231.54.234",
           "br_features_pdf" : true,
           "page_referrer" : null,
           "ua_parser_context_useragentPatch" : null,
           "doc_height" : null,
           "refr_urlscheme" : null,
           "performance_timing_redirectStart" : 0,
           "geo_region" : "TX",
           "geo_timezone" : null,
           "page_urlfragment" : "4-conclusion",
           "br_features_flash" : false,
           "os_manufacturer" : null,
           "mkt_clickid" : null,
           "ti_price" : null,
           "br_colordepth" : null,
           "web_page_author" : "Fred Blundun",
           "event_format" : "jsonschema",
           "tr_total" : null,
           "pp_xoffset_min" : null,
           "doc_width" : null,
           "geo_zipcode" : "94109",
           "br_family" : null,
           "web_page_datePublished" : "2014-11-06T00:00:00Z",
           "tr_currency" : null,
           "web_page_breadcrumb" : "blog",
           "useragent" : null,
           "event_name" : "link_click_test",
           "os_name" : null,
           "page_urlpath" : "/product/index.html",
           "br_name" : null,
           "ip_netspeed" : "Cable/DSL",
           "page_title" : "On Analytics",
           "performance_timing_navigationStart" : 1415358089861,
           "ip_organization" : "Bouygues Telecom",
           "performance_timing_domInteractive" : 1415358090886,
           "dvce_created_tstamp" : "2018-07-20T00:03:57.885Z",
           "br_features_gears" : null,
           "dvce_type" : null,
           "dvce_sent_tstamp" : null,
           "se_action" : null,
           "br_features_director" : null,
           "performance_timing_domComplete" : 0,
           "se_category" : null,
           "ti_name" : null,
           "user_id" : "jon.doe@email.com",
           "performance_timing_unloadEventStart" : 1415358090270,
           "refr_urlquery" : null,
           "ua_parser_context_useragentVersion" : "IE 7.0",
           "performance_timing_loadEventStart" : 0,
           "performance_timing_domLoading" : 1415358090270,
           "true_tstamp" : "2018-07-23T00:03:57.886Z",
           "geo_longitude" : -122.4124,
           "mkt_term" : null,
           "v_tracker" : "js-2.1.0",
           "os_timezone" : null,
           "br_type" : null,
           "br_features_windowsmedia" : null,
           "link_click_targetUrl" : "http://www.example.com",
           "event_version" : "1-0-0",
           "ua_parser_context_useragentMajor" : "7",
           "dvce_screenwidth" : null,
           "se_label" : null,
           "domain_sessionid" : "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
           "performance_timing_connectStart" : 1415358090103,
           "performance_timing_fetchStart" : 1415358089870,
           "domain_userid" : "bc2e92ec6c204a14",
           "page_urlquery" : "id=GTM-DLRG",
           "geo_location" : "37.443604,-122.4124",
           "refr_term" : null,
           "refr_device_tstamp" : null,
           "link_click_elementClasses" : "foreground",
           "link_click_elementId" : "exampleLink",
           "name_tracker" : "cloudfront-1",
           "ua_parser_context_useragentMinor" : "0",
           "tr_tax_base" : null,
           "web_page_keywords" : "snowplow",
           "dvce_screenheight" : null,
           "mkt_campaign" : null,
           "refr_urlfragment" : null,
           "performance_timing_domContentLoadedEventStart" : 1415358090968,
           "tr_shipping" : null,
           "tr_shipping_base" : null,
           "br_features_java" : null,
           "br_viewwidth" : null,
           "geo_city" : "New York",
           "web_page_genre" : "blog",
           "br_viewheight" : null,
           "refr_domain_userid" : null,
           "br_features_silverlight" : null,
           "ti_price_base" : null,
           "tr_tax" : null,
           "br_cookies" : null,
           "tr_total_base" : null,
           "refr_urlport" : null,
           "derived_tstamp" : "2018-07-22T00:03:57.886Z",
           "app_id" : "angry-birds",
           "ip_isp" : "FDN Communications",
           "ua_parser_context_deviceFamily" : "Other",
           "geo_region_name" : "Florida",
           "ua_parser_context_osPatch" : null,
           "pp_yoffset_max" : null,
           "ip_domain" : "nuvox.net",
           "performance_timing_connectEnd" : 1415358090183,
           "domain_sessionidx" : 3,
           "pp_xoffset_max" : null,
           "mkt_source" : null,
           "page_urlport" : 80,
           "se_property" : null,
           "platform" : "web",
           "ua_parser_context_osFamily" : "Windows XP",
           "performance_timing_loadEventEnd" : 0,
           "event_id" : "c6124-b53a-4b13-a233-0088f79dcbcb",
           "refr_urlpath" : null,
           "mkt_network" : null,
           "performance_timing_redirectEnd" : 0,
           "ua_parser_context_osMajor" : null,
           "ua_parser_context_osPatchMinor" : null,
           "ua_parser_context_osVersion" : "Windows XP",
           "performance_timing_domainLookupEnd" : 1415358090102,
           "se_value" : null,
           "page_url" : "http://www.snowplowanalytics.com",
           "etl_tags" : null,
           "tr_orderid" : null,
           "tr_state" : null,
           "txn_id" : 41828,
           "performance_timing_responseEnd" : 1415358090265,
           "refr_source" : null,
           "tr_country" : null,
           "tr_city" : null,
           "doc_charset" : null,
           "event_fingerprint" : "e3dbfa9cca0412c3d4052863cefb547f",
           "v_etl" : "serde-0.5.2"
         }
      }
    """

    val result = (for {
      snowplowEvent <- EitherT.fromEither[Option](
        EventTransformer
          .transformWithInventory(Instances.tsvInput)
          .leftMap(errors => TransformationError(errors.mkString("\n  * "))))
      indicativeEvent <- EitherT(Transformer.transform(snowplowEvent.event, snowplowEvent.inventory))
    } yield indicativeEvent).value

    result shouldEqual Some(Right(expected.asObject.get))

  }

  def e2 = {
    val event = Instances.input
      .map {
        case (fieldName, _) if List("user_id", "domain_userid").contains(fieldName) => fieldName -> ""
        case a                                                                      => a
      }
      .unzip
      ._2
      .mkString("\t")
    val result = (for {
      snowplowEvent <- EitherT.fromEither[Option](
        EventTransformer
          .transformWithInventory(event)
          .leftMap(errors => TransformationError(errors.mkString("\n  * "))))
      indicativeEvent <- EitherT(Transformer.transform(snowplowEvent.event, snowplowEvent.inventory))
    } yield indicativeEvent).value

    println(result)
    result must_== None
  }

  def e3 = {
    val uris = List(
      "iglu:com.getvero/delivered/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.snowplow.enrichments/weather_enrichment_config/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.snowplow/ad_conversion/jsonschema/1-0-0"
    )

    Result.foreach(uris)(runTest)
  }

  def e4 = {
    val input = json"""
      {
        "foo": []
      }
    """

    FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
  }

  def e5 = {
    val input = json"""
      {
        "foo": {}
      }
    """

    FieldsExtraction.flattenJson(input, Set.empty) shouldEqual Map.empty
  }

}
