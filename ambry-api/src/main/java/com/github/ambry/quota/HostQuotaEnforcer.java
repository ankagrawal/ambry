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
 * Interface for class that would do the quota enforcement based on quota of load on host resources. This type of quota
 * enforcement doesn't need details about the request and can be done much before than deserialization of the request.
 * A {@link HostQuotaEnforcer} object would need a {@link QuotaSource} to get and save quota and usage.
 */
public interface HostQuotaEnforcer {
  /**
   * Method to initialize the {@link HostQuotaEnforcer}.
   */
  void init();

  /**
   * Makes an {@link EnforcementRecommendation} based on quota on host load.
   * @return EnforcementRecommendation object with the recommendation.
   */
  EnforcementRecommendation recommend();

  /**
   * Shutdown the {@link HostQuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}
