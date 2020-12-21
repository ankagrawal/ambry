/*
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

import com.github.ambry.quota.storage.QuotaOperation;


/**
 * Interface representing the backend source from which quota for a resource can be fetched, and to which the current
 * usage of a resource can be saved.
 */
public interface QuotaSource {
  /**
   * Get the {@link Quota} for specified resource and operation.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaOperation {@link QuotaOperation} object.
   * @param quotaMetric {@link QuotaMetric} object.
   */
  Quota getQuota(QuotaResource quotaResource, QuotaOperation quotaOperation, QuotaMetric quotaMetric);
}