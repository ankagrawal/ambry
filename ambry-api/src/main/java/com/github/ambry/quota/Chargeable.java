/**
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

/**
 * A {@link Chargeable} is an operation that can be charged against quota.
 */
public interface Chargeable {

  /**
   * Charge the request cost for this operation against quota of the quota resource of this operation.
   *
   * @param shouldCheckExceedAllowed if {@code true} then it should be checked if usage is allowed to exceed quota.
   * @return QuotaAction representing the recommended action to take.
   */
  boolean checkAndCharge(boolean shouldCheckExceedAllowed) throws QuotaException;

  /**
   * @return the {@link QuotaResource} whose operation is being charged.
   */
  QuotaResource getQuotaResource();

  /**
   * @return the {@link QuotaMethod} of the request for which quota is being charged.
   */
  QuotaMethod getQuotaMethod();
}
