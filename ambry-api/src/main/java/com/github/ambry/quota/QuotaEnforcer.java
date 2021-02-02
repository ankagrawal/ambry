/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;


/**
 * Interface for class that would do the quota enforcement of a particular quota.
 * A {@link QuotaEnforcer} object would need a {@link QuotaSource} to get and save quota and usage.
 */
public interface QuotaEnforcer {
  /**
   * Method to initialize the {@link QuotaEnforcer}.
   */
  void init();

  /**
   * Makes an {@link EnforcementRecommendation} using the information in {@link BlobInfo} and {@link RestRequest}. This
   * method also charges the request cost against the quota.
   * @param restRequest {@link RestRequest} object.
   * @param blobInfo {@link BlobInfo} object representing the blob characteristics using which request cost can be
   *                                 determined by enforcers.
   * @return EnforcementRecommendation object with the recommendation.
   */
  EnforcementRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo);

  /**
   * Makes an {@link EnforcementRecommendation} for the restRequest. This method doesn't know the
   * request details and hence makes the recommendation based on current quota usage.
   * @param restRequest {@link RestRequest} object.
   * @return EnforcementRecommendation object with the recommendation.
   */
  EnforcementRecommendation recommend(RestRequest restRequest);

  /**
   * @return QuotaSource object of the enforcer.
   */
  QuotaSource getQuotaSource();

  /**
   * Shutdown the {@link QuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}