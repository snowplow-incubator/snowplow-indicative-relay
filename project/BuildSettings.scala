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
import Keys._

// Scalafmt plugin
import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport._

// SBT Assembly
import sbtassembly._
import sbtassembly.AssemblyKeys._

// Buildinfo plugin
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoKeys._

object BuildSettings {

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  lazy val formatting = Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true
  )

  lazy val assemblyOptions = Seq(
    assembly / target := file("target/scala-2.11/assembled_jars/"),
    assembly / assemblyJarName := { name.value + "-" + version.value + ".jar" },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

  lazy val buildInfo = Seq(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.snowplowanalytics.indicative"
  )

}
