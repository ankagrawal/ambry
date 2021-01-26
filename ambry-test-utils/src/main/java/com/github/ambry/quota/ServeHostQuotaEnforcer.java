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

/**
 * {@link RequestQuotaEnforcer} implementation for test that always servers requests.
 */
public class ServeHostQuotaEnforcer implements HostQuotaEnforcer {
  private static final float DUMMY_SERVABLE_USAGE_PERCENTAGE = 10;
  private static final int SERVER_HTTP_STATUS = 200;
  private static final boolean SHOULD_THROTTLE = false;

  @Override
  public void init() {
  }

  @Override
  public EnforcementRecommendation recommend(String hostName) {
    return new AmbryEnforcementRecommendation(SHOULD_THROTTLE, DUMMY_SERVABLE_USAGE_PERCENTAGE,
        ServeHostQuotaEnforcer.class.getSimpleName(), SERVER_HTTP_STATUS, null);
  }

  @Override
  public void shutdown() {
  }
}
