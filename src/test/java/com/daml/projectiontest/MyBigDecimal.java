// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projectiontest;

import java.math.BigDecimal;

public class MyBigDecimal extends BigDecimal {
  public MyBigDecimal(String val) {
    super(val);
  }
}
