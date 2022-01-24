/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.quota.capacityunit;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ambry.quota.QuotaName;
import com.google.common.util.concurrent.AtomicDouble;
import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * Class encapsulating the Read_Capacity_Unit and Write_Capacity_Unit quotas or usage for resource.
 */
public class CapacityUnit {
  static final String RCU_FIELD_NAME = "rcu";
  static final String WCU_FIELD_NAME = "wcu";

  private final AtomicDouble rcu;
  private final AtomicDouble wcu;

  /**
   * Constructor for {@link CapacityUnit}.
   */
  public CapacityUnit() {
    rcu = new AtomicDouble(0);
    wcu = new AtomicDouble(0);
  }

  @JsonIgnore
  public CapacityUnit(JsonNode jsonNode) {
    this.rcu = new AtomicDouble(jsonNode.get(RCU_FIELD_NAME).asLong());
    this.wcu = new AtomicDouble(jsonNode.get(WCU_FIELD_NAME).asLong());
  }

  @JsonIgnore
  public static boolean isQuotaNode(JsonNode jsonNode) {
    return jsonNode.has(WCU_FIELD_NAME);
  }

  @JsonIgnore
  public double getQuotaValue(QuotaName quotaName) {
    switch (quotaName) {
      case READ_CAPACITY_UNIT:
        return rcu.get();
      case WRITE_CAPACITY_UNIT:
        return wcu.get();
      default:
        throw new IllegalArgumentException("Invalid quota name: " + quotaName.name());
    }
  }

  /**
   * @return Read Capacity Unit quota value.
   */
  public double getRcu() {
    return rcu.get();
  }

  /**
   * Set the Read Capacity Unit quota to the specified value.
   */
  public void setRcu(long rcu) {
    this.rcu.set(rcu);
  }

  /**
   * @return Write Capacity Unit quota value.
   */
  public double getWcu() {
    return wcu.get();
  }

  /**
   * Set the Write Capacity Unit quota to the specified value.
   */
  public void setWcu(long wcu) {
    this.wcu.set(wcu);
  }
}
