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

/**
 * Enum representing various types of quota metrics supported. It would be used by {@link QuotaSource} to get the
 * appropriate quota values.
 */
public enum QuotaMetric {
  CAPACITY_UNIT,
  STORAGE_IN_GB,
  HOST_CPU,
  HOST_HEAP_MEMORY,
  HOST_DIRECT_MEMORY,
  HOST_DISK_CAPACITY
}
