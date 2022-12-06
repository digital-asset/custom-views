// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection
package javadsl

import com.daml.projection.Sql.NamedParameter
import org.scalatest.matchers.must._
import org.scalatest.wordspec._

class ExecuteUpdateSpec
    extends AnyWordSpecLike
    with Matchers {
  "ExecuteUpdate" must {
    "support a query without bind parameters" in {
      val query = "select * from table"
      val update = ExecuteUpdate.create(query)
      update.sql.namedParameters must be(empty)
      update.sql.jdbcStr must be(query)
    }
    "support a query with a named bind parameter" in {
      val query = "select * from table where id = :id"
      val update = ExecuteUpdate.create(query)
      update.sql.namedParameters must be(Map("id" -> NamedParameter("id", 1)))
      update.sql.jdbcStr must be("select * from table where id = ?")
      val bound = update.bind("id", "my-id")
      bound.sql.jdbcStr must be(update.sql.jdbcStr)
      bound.sql.setters(1) must be(BindValue("my-id", 1))
    }

    "Only support bind parameters starting with [a-z]+, followed by _, [a-z]+, or [0-9]" in {
      val query = "select * from table where id = :1"
      val update = ExecuteUpdate.create(query)
      update.sql.namedParameters must be(empty)
      an[IllegalArgumentException] must be thrownBy update.bind("1", "value")
      an[IllegalArgumentException] must be thrownBy update.bind("2a", "value")
      an[IllegalArgumentException] must be thrownBy update.bind("3_a", "value")
      ExecuteUpdate.create("select * from table where id = :a1").sql.parameterNames must be(List("a1"))
      ExecuteUpdate.create("select * from table where id = :a_1").sql.parameterNames must be(List("a_1"))
      ExecuteUpdate.create("select * from table where id = :a_1b").sql.parameterNames must be(List("a_1b"))
      ExecuteUpdate.create("select * from table where id = :abc_def_1b").sql.parameterNames must be(List("abc_def_1b"))
    }

    "support a query with named bind parameters" in {
      val query = "select * from table where id = :id order by :order"
      val update = ExecuteUpdate.create(query)
      update.sql.parameterNames must be(List("id", "order"))
      update.sql.namedParameters must contain theSameElementsAs (Map(
        "id" -> NamedParameter("id", 1),
        "order" -> NamedParameter("order", 2)))
      update.sql.jdbcStr must be("select * from table where id = ? order by ?")
      val bound = update
        .bind("id", "my-id")
        .bind("order", "asc")
      bound.sql.jdbcStr must be(update.sql.jdbcStr)
      bound.sql.setters(1) must be(BindValue("my-id", 1))
      bound.sql.setters(2) must be(BindValue("asc", 2))
    }

    "support a query mixed named and positional bind parameters" in {
      val query = "select * from table where id = :id and name = ? order by :order"
      val update = ExecuteUpdate.create(query)
      update.sql.parameterNames must be(List("id", "order"))
      update.sql.namedParameters must contain theSameElementsAs (Map(
        "id" -> NamedParameter("id", 1),
        "order" -> NamedParameter("order", 3)))
      update.sql.jdbcStr must be("select * from table where id = ? and name = ? order by ?")
      val bound = update
        .bind("id", "my-id")
        .bind("order", "asc")
        .bind(2, "name")
      bound.sql.jdbcStr must be(update.sql.jdbcStr)
      bound.sql.setters(1) must be(BindValue("my-id", 1))
      bound.sql.setters(2) must be(BindValue("name", 2))
      bound.sql.setters(3) must be(BindValue("asc", 3))
    }

    "support a query with named bind parameters and postgres casts " in {
      val query = "insert into table t(id, json1, json2) values(:id, ?::jsonb, ? :: jsonb)"
      val update = ExecuteUpdate.create(query)
      update.sql.parameterNames must be(List("id"))
      update.sql.namedParameters must contain theSameElementsAs (Map("id" -> NamedParameter("id", 1)))
      update.sql.jdbcStr must be("insert into table t(id, json1, json2) values(?, ?::jsonb, ? :: jsonb)")
      val bound = update
        .bind("id", "my-id")
      bound.sql.jdbcStr must be(update.sql.jdbcStr)
      bound.sql.setters(1) must be(BindValue("my-id", 1))
    }
    "" in {
      """select '{"foo": {"bar": "baz"}}'::jsonb ? 'bar' """
    }
    "fail bind for wrong name" in {
      val query = "select * from table where id = :id order by :order"
      val update = ExecuteUpdate.create(query)
      an[IllegalArgumentException] must be thrownBy update.bind("name", "my-name")
      an[IllegalArgumentException] must be thrownBy update.bind("id", "my-id").bind("id", "my-id-again")
      an[IllegalArgumentException] must be thrownBy update.bind("id", "my-id").bind("name", "my-name")
    }

    "fail bind for wrong position" in {
      val query = "select * from table where id = :id order by :order"
      val update = ExecuteUpdate.create(query)
      an[IllegalArgumentException] must be thrownBy update.bind(-1, "my-name")
      an[IllegalArgumentException] must be thrownBy update.bind(0, "my-name")
      an[IllegalArgumentException] must be thrownBy update.bind(3, "my-name")
      an[IllegalArgumentException] must be thrownBy update.bind(1, "my-name").bind(1, "again")
    }

    "support bind by position" in {
      val query = "select * from table where id = :id and name=:name"
      val update = ExecuteUpdate.create(query)
      update.sql.namedParameters must contain theSameElementsAs (Map(
        "id" -> NamedParameter("id", 1),
        "name" -> NamedParameter("name", 2)))
      update.sql.jdbcStr must be("select * from table where id = ? and name=?")
      val bound = update.bind(1, "my-id").bind(2, "my-name")
      bound.sql.jdbcStr must be(update.sql.jdbcStr)
      bound.sql.setters(1) must be(BindValue("my-id", 1))
      bound.sql.setters(2) must be(BindValue("my-name", 2))
    }
    "support bind by position using ?" in {
      val query = "select * from table where id = ? and name = ?"
      val update = ExecuteUpdate.create(query)
      update.sql.jdbcStr must be("select * from table where id = ? and name = ?")
      val bound = update.bind(1, "my-id").bind(2, "my-name")
      bound.sql.jdbcStr must be(update.sql.jdbcStr)
    }
  }
}
