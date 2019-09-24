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
import sbt._

object Dependencies {

  object V {
    val awsLambdaCore    = "1.2.0"
    val awsLambdaEvents  = "2.2.7"
    val kinesisClient    = "1.11.2"
    val analyticsSdk     = "0.3.2"
    val circe            = "0.11.1"
    val catsEffect       = "1.4.0"
    val scalaj           = "2.4.2"
    val specs2           = "4.5.1"
    val scalacheckSchema = "0.1.0"
  }

  object Libraries {
    //Java
    val awsLambdaCore    = "com.amazonaws"          % "aws-lambda-java-core"         % V.awsLambdaCore
    val awsLambdaEvents  = "com.amazonaws"          % "aws-lambda-java-events"       % V.awsLambdaEvents
    val kinesisClient    = "com.amazonaws"          % "amazon-kinesis-client"        % V.kinesisClient
    // Scala
    val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
    val circeCore        = "io.circe"              %% "circe-core"                   % V.circe
    val circeParser      = "io.circe"              %% "circe-parser"                 % V.circe
    val catsEffect       = "org.typelevel"         %% "cats-effect"                  % V.catsEffect
    val scalaj           = "org.scalaj"            %% "scalaj-http"                  % V.scalaj
    // Tests
    val circeLiteral     = "io.circe"              %% "circe-literal"                % V.circe  % "test"
    val specs2           = "org.specs2"            %% "specs2-core"                  % V.specs2 % "test"
    val specs2ScalaCheck = "org.specs2"            %% "specs2-scalacheck"            % V.specs2 % "test"
    val scalacheckSchema = "com.snowplowanalytics" %% "scalacheck-schema"            % V.scalacheckSchema % "test"
  }

}
