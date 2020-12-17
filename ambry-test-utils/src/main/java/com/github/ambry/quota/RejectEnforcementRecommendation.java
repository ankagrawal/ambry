/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

public class RejectEnforcementRecommendation implements EnforcementRecommendation {
  private static final int REJECT_HTTP_STATUS_CODE = 429;
  private final String enforcerName;

  public RejectEnforcementRecommendation(String enforcerName) {
    this.enforcerName = enforcerName;
  }

  @Override
  public boolean shouldThrottle() {
    return true;
  }

  @Override
  public float quotaUsagePercentage() {
    return 101;
  }

  @Override
  public String getQuotaEnforcerName() {
    return enforcerName;
  }

  @Override
  public int getRecommendedHttpStatus() {
    return REJECT_HTTP_STATUS_CODE;
  }
}
