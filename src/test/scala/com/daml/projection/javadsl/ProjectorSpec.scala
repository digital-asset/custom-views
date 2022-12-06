// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.javadsl

import akka.actor.ActorSystem
import akka.stream.ActorAttributes.Dispatcher
import akka.testkit.TestKit
import com.daml.projection.{ SandboxHelper, TestEmbeddedPostgres }
import org.scalatest.matchers.must._
import org.scalatest.wordspec._

class ProjectorSpec
    extends TestKit(ActorSystem("ProjectorSpec"))
    with AnyWordSpecLike
    with Matchers
    with TestEmbeddedPostgres
    with SandboxHelper {
  "A Jdbc Projector Flow" must {
    "have a dispatcher dedicated to blocking operations specified via attributes" in {
      val projector = JdbcProjector.create(
        ds,
        system
      )
      val flow = projector.flow

      flow.getAttributes.attributeList must be(
        List(Dispatcher("projection.blocking-io-dispatcher"))
      )
    }
  }
}
