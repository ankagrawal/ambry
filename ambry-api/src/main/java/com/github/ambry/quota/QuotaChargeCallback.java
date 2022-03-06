/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.rest.RestRequest;


/**
 * Callback for charging request cost against quota. Used by {@link QuotaEnforcer}s to charge quota for a request.
 */
public interface QuotaChargeCallback {

  /**
   * Callback method that can be used to charge quota usage for a request or part of a request.
   * @param chunkSize of the chunk.
   * @throws QuotaException In case request needs to be throttled.
   * @return
   */
  QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge, long chunkSize) throws QuotaException;

  /**
   * Callback method that can be used to charge quota usage for a request or part of a request. Call this method
   * when the quota charge doesn't depend on the chunk size.
   * @throws QuotaException In case request needs to be throttled.
   */
  QuotaAction checkAndCharge(boolean shouldCheckExceedAllowed, boolean forceCharge) throws QuotaException;

  /**
   * @return QuotaResource object.
   * @throws QuotaException in case of any errors.
   */
  QuotaResource getQuotaResource() throws QuotaException;

  /**
   * @return QuotaMethod object.
   */
  QuotaMethod getQuotaMethod();

  /**
   * @return QuotaConfig object.
   */
  QuotaConfig getQuotaConfig();
}
