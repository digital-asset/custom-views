// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

scalaVersion := "2.13.8"
// Overrides sbt-dynver version
version := sys.env.get("VERSION").getOrElse("LOCAL-SNAPSHOT")
organization := "com.daml"
organizationName := "Digital Asset"
startYear := Some(2022)
licenses += ("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://docs.daml.com"))

headerLicense := Some(
  HeaderLicense.Custom(
    """|Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
     |SPDX-License-Identifier: Apache-2.0
     |""".stripMargin
  )
)
headerMappings := headerMappings.value + (
  HeaderFileType.scala -> HeaderCommentStyle.cppStyleLineComment,
  HeaderFileType.java -> HeaderCommentStyle.cppStyleLineComment,
)

sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
// Note: the built-in `developers` list does not contain the same information
// we published from the daml repo; manually setting for consistency
pomExtra := <developers>
    <developer>
      <name>Digital Asset SDK Feedback</name>
      <email>sdk-feedback@digitalasset.com</email>
      <organization>Digital Asset (Switzerland) GmbH</organization>
      <organizationUrl>https://www.digitalasset.com/developers</organizationUrl>
    </developer>
  </developers>

val AkkaVersion = "2.6.20"
val ScalaTestVersion = "3.2.12"
val DoobieVersion = "1.0.0-RC2"

val DamlVersion = "2.5.0-snapshot.20221120.10983.0.218a6a8a"

val deps = Seq(
  "com.daml" % "bindings-java" % DamlVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % AkkaVersion,
  "org.flywaydb" % "flyway-core" % "8.5.11",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.slf4j" % "slf4j-api" % "2.0.5",
  "ch.qos.logback" % "logback-classic" % "1.3.5",
  "com.daml" % "ledger-api-proto" % DamlVersion % "protobuf-src",
  "com.zaxxer" % "HikariCP" % "5.0.1" % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
  "org.scalatest" %% "scalatest-core" % ScalaTestVersion % Test,
  "org.scalatest" %% "scalatest-matchers-core" % ScalaTestVersion % Test,
  "org.scalatest" %% "scalatest-mustmatchers" % ScalaTestVersion % Test,
  "org.scalatest" %% "scalatest-wordspec" % ScalaTestVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.10" % Test,
  "com.opentable.components" % "otj-pg-embedded" % "1.0.1" % Test,
  "org.testcontainers" % "testcontainers" % "1.17.6" % Test,
  "org.tpolecat" %% "doobie-postgres" % DoobieVersion % Test,
  "org.tpolecat" %% "doobie-core" % DoobieVersion % Test,
  "org.tpolecat" %% "doobie-hikari" % DoobieVersion % Test,
  "com.daml" % "bindings-rxjava" % DamlVersion % Test,
  ("com.daml" %% "sandbox-on-x" % DamlVersion % Test).exclude("org.slf4j", "slf4j-api")
)

val scalacOpts =
  // Fail the compilation if there are any warnings, unless no.discipline is set to any non-empty string.
  sys.props.get("no.discipline").map(_ => Seq()).getOrElse(Seq("-Werror")) ++
    Seq(
      "-target:jvm-11",
      "-encoding",
      "utf-8",
      // Silence warnings for generated code, Emit warning and location for usages of deprecated APIs except for the generated ledger API.
      """-Wconf:src=akka-grpc/.*:silent,cat=deprecation&origin=com\.daml\.ledger\.api\.v1\..*:silent,cat=deprecation&origin=com\.daml\.projection\..*:w""",
      // Explain type errors in more detail.
      "-explaintypes",
      // Emit warning and location for usages of features that should be imported explicitly.
      "-feature",
      // Enable additional warnings where generated code depends on assumptions.
      "-unchecked",
      // removing by-name conversion of block result because of doobie
      "-Xlint:-byname-implicit",
      // Warn if an argument list is modified to match the receiver.
      "-Xlint:adapted-args",
      // Evaluation of a constant arithmetic expression results in an error.
      "-Xlint:constant",
      // Selecting member of DelayedInit.
      "-Xlint:delayedinit-select",
      // A Scaladoc comment appears to be detached from its element.
      "-Xlint:doc-detached",
      // Warn about inaccessible types in method signatures.
      "-Xlint:inaccessible",
      // Warn when a type argument is inferred to be `Any`.
      "-Xlint:infer-any",
      // A string literal appears to be missing an interpolator id.
      "-Xlint:missing-interpolator",
      // Warn when nullary methods return Unit.
      "-Xlint:nullary-unit",
      // Option.apply used implicit view.
      "-Xlint:option-implicit",
      // Class or object defined in package object.
      "-Xlint:package-object-classes",
      // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:poly-implicit-overload",
      // A private field (or class parameter) shadows a superclass field.
      "-Xlint:private-shadow",
      // Pattern sequence wildcard must align with sequence component.
      "-Xlint:stars-align",
      // A local type parameter shadows a type already in scope.
      "-Xlint:type-parameter-shadow",
      // Warn when dead code is identified.
      "-Ywarn-dead-code",
      // Warn when more than one implicit parameter section is defined.
      "-Ywarn-extra-implicit",
      // Warn when numerics are widened.
      "-Ywarn-numeric-widen",
      // Warn if an implicit parameter is unused.
      "-Ywarn-unused:implicits",
      // Warn if an import selector is not referenced.
      "-Ywarn-unused:imports",
      // Warn if a local definition is unused.
      "-Ywarn-unused:locals",
      // Warn if a @nowarn annotation does not suppress any warnings.
      "-Ywarn-unused:nowarn",
      // Warn if a value parameter is unused.
      "-Ywarn-unused:params",
      // Warn if a variable bound in a pattern is unused.
      "-Ywarn-unused:patvars",
      // Warn if a private member is unused.
      "-Ywarn-unused:privates",
      // Warn when non-Unit expression results are unused.
      "-Ywarn-value-discard",
      // Avoid "Exhaustivity analysis reached max recursion depth".
      "-Ypatmat-exhaust-depth",
      "80"
    )

lazy val projection = (project in file("."))
  .enablePlugins(AkkaGrpcPlugin)
  .enablePlugins(AutomateHeaderPlugin)
  .configs(PerfTest)
  .settings(
    headerSources / excludeFilter := HiddenFileFilter || "TestDB.scala" || "TestEmbeddedPostgres.scala",
    name := "custom-views",
    Compile / scalacOptions := scalacOpts,
    akkaGrpcGeneratedSources := Seq(AkkaGrpc.Client),
    akkaGrpcGeneratedLanguages := Seq(AkkaGrpc.Scala, AkkaGrpc.Java),
    akkaGrpcCodeGeneratorSettings := akkaGrpcCodeGeneratorSettings.value
      .filterNot(_ == "flat_package"),
    akkaGrpcCodeGeneratorSettings += "java_conversions",
    inConfig(PerfTest)(Defaults.testTasks),
    Test / testOptions := Seq(Tests.Filter(unitFilter)),
    PerfTest / testOptions := Seq(Tests.Filter(perfFilter)),
    libraryDependencies ++= deps,
    Test / fork := true,
    automateHeaderSettings(PerfTest),
    inConfig(PerfTest)(JavaFormatterPlugin.toBeScopedSettings)
  )
lazy val PerfTest = config("perf").extend(Test)
def perfFilter(name: String): Boolean = name.endsWith("PerfSpec")
def unitFilter(name: String): Boolean = (name.endsWith("Spec")) && !perfFilter(name)
