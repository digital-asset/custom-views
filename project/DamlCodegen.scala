package com.daml.projection
import sbt._
import sbt.Keys._
import sbt.Keys.streams

import scala.sys.process.Process
import scala.util.control.NoStackTrace

object DamlCodegen {
  def task = Def.task {
    val workingDir = baseDirectory.value / "src" / "test" / "daml"
    val damlSourceManagedDir = (Test / sourceManaged).value / "daml"
    val darTargetDir = target.value / "daml"
    val darName = "quickstart.dar"
    val builtDar = darTargetDir / darName

    val buildExitCode = Process("daml" :: "build" :: "-o" :: builtDar.getAbsolutePath :: Nil, workingDir).!
    if (buildExitCode != 0) {
      throw DamlCodegenError(s"Failed to build dar: ${builtDar.getAbsolutePath}")
    }

    val codegenExitCode = Process(
      "daml" :: "codegen" :: "java" ::
        "-o" :: damlSourceManagedDir.getAbsolutePath ::
        s"${builtDar.getAbsolutePath}=com.daml.quickstart.model" ::
        "-d" :: "com.daml.quickstart.iou.TemplateDecoder" :: Nil,
      workingDir
    ).!
    if (codegenExitCode != 0) {
      throw DamlCodegenError(s"Failed to generate Java code from: ${builtDar.getAbsolutePath}")
    }
    damlSourceManagedDir.allPaths.get().filter(_.isFile)
  }
}

case class DamlCodegenError(msg: String)
    extends Exception(s"\n$msg")
    with NoStackTrace
    with sbt.FeedbackProvidedException
