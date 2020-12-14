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

import com.github.ambry.config.QuotaConfig;


/**
 * Factory to instantiate {@link QuotaManager}.
 */
public interface QuotaManagerFactory {

  /**
   * Build and return the {@link QuotaManager} object.
   * @param quotaConfig {@link QuotaConfig} object.
   * @return QuotaManager object.
   * @throws ReflectiveOperationException in case of exception.
   */
  QuotaManager getQuotaManager(QuotaConfig quotaConfig) throws ReflectiveOperationException;
}
