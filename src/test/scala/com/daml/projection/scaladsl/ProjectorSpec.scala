// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorAttributes.Dispatcher
import akka.testkit.TestKit
import cats.effect.unsafe.implicits.global
import com.daml.projection.TestEmbeddedPostgres
import org.scalatest.matchers.must._
import org.scalatest.wordspec._

class ProjectorSpec
    extends TestKit(ActorSystem("ProjectionSpec"))
    with AnyWordSpecLike
    with Matchers
    with TestEmbeddedPostgres {
  "A Doobie Projector Flow" must {
    "have a dispatcher dedicated to blocking operations specified via attributes" in {
      val flow = Doobie.Projector().flow

      flow.getAttributes.attributeList must be(
        List(Dispatcher("projection.blocking-io-dispatcher"))
      )
    }
  }
}
