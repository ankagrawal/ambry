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
package com.github.ambry.router;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSource;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory;
import com.github.ambry.quota.capacityunit.CapacityUnit;
import java.io.IOException;
import java.util.Map;

public class TestCUQuotaSourceFactory implements QuotaSourceFactory {
  private final QuotaConfig quotaConfig;
  private final AccountService accountService;

  /**
   * Constructor for {@link AmbryCUQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   */
  public TestCUQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService) {
    this.quotaConfig = quotaConfig;
    this.accountService = accountService;
  }

  @Override
  public QuotaSource getQuotaSource() {
    try {
      return new TestCUQuotaSource(quotaConfig, accountService);
    } catch (IOException ioException) {
      return null;
    }
  }

}

class TestCUQuotaSource extends AmbryCUQuotaSource {
  public TestCUQuotaSource(QuotaConfig quotaConfig, AccountService accountService) throws IOException {
    super(quotaConfig, accountService);
  }

  public void setFeQuota(CapacityUnit newFeQuota) {
    feQuota.setWcu(newFeQuota.getWcu());
    feQuota.setRcu(newFeQuota.getRcu());
  }

  public CapacityUnit getFeQuota() {
    return feQuota;
  }

  public void setFeUsage(CapacityUnit newFeUsage) {
    feUsage.get().setRcu(newFeUsage.getRcu());
    feUsage.get().setWcu(newFeUsage.getWcu());
  }

  public void setCuQuota(Map<String, CapacityUnit> newCuQuota) {
    cuQuota.putAll(newCuQuota);
  }

  public Map<String, CapacityUnit> getCuQuota() {
    return cuQuota;
  }

  public void setCuUsage(Map<String, CapacityUnit> newCuUsage) {
    cuUsage.putAll(newCuUsage);
  }

  public Map<String, CapacityUnit> getCuUsage() {
    return cuUsage;
  }
}
