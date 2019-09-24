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
lazy val root = project
  .in(file("."))
  .settings(
    name := "indicative-relay",
    organization := "com.snowplowanalytics",
    version := "0.4.0",
    description := "A relay transforming Snowplow enriched events into Indicative format",
    scalaVersion := "2.11.12",
    javacOptions := BuildSettings.javaCompilerOptions
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.buildInfo)
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.assemblyOptions)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.awsLambdaCore,
      Dependencies.Libraries.awsLambdaEvents,
      Dependencies.Libraries.kinesisClient,
      Dependencies.Libraries.analyticsSdk,
      Dependencies.Libraries.circeCore,
      Dependencies.Libraries.circeParser,
      Dependencies.Libraries.catsEffect,
      Dependencies.Libraries.scalaj,
      Dependencies.Libraries.circeLiteral,
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.specs2ScalaCheck,
      Dependencies.Libraries.scalacheckSchema
    )
  )
