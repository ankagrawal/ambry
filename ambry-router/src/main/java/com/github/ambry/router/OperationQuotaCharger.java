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
package com.github.ambry.router;

import com.github.ambry.commons.BlobId;
import com.github.ambry.quota.Chargeable;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link Chargeable} implementation for cases (Undelete, Delete, UpdateTtl, GetBlobInfo) where quota is charged just once for entire operation.
 */
public class OperationQuotaCharger implements Chargeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OperationQuotaCharger.class);
  private final QuotaChargeCallback quotaChargeCallback;
  private final BlobId blobId;
  private final String operationName;
  private boolean isCharged;

  /**
   * Constructor for {@link OperationQuotaCharger}.
   *
   * @param quotaChargeCallback {@link QuotaChargeCallback} object to chargeIfUsageWithinQuota and check quotas.
   * @param blobId {@link BlobId} of the blob for which quota will be charged.
   * @param operationName Name of the operation.
   */
  public OperationQuotaCharger(QuotaChargeCallback quotaChargeCallback, BlobId blobId, String operationName) {
    this.quotaChargeCallback = quotaChargeCallback;
    this.blobId = blobId;
    this.operationName = operationName;
    this.isCharged = false;
  }

  @Override
  public boolean check() {
    if (quotaChargeCallback == null || isCharged) {
      return true;
    }
    return quotaChargeCallback.check();
  }

  @Override
  public boolean checkAndCharge() {
    if (quotaChargeCallback == null || isCharged) {
      return true;
    }
    try {
      isCharged = quotaChargeCallback.checkAndCharge();
    } catch (Exception ex) {
      LOGGER.warn(String.format("Quota charging failed in %s for blob %s due to %s ", operationName, blobId.toString(),
          ex.toString()));
    }
    return isCharged;
  }

  @Override
  public boolean chargeIfQuotaExceedAllowed() {
    if (quotaChargeCallback == null) {
      return true;
    }
    try {
      isCharged = quotaChargeCallback.chargeIfQuotaExceedAllowed();
    } catch (Exception ex) {
      LOGGER.warn(String.format("Quota charging failed in %s for blob %s due to %s ", operationName, blobId.toString(),
          ex.toString()));
      // In case of exception we don't set isCharged but let the request go through.
      return true;
    }
    return isCharged;
  }

  @Override
  public QuotaResource getQuotaResource() {
    if (quotaChargeCallback == null) {
      return null;
    }
    try {
      return quotaChargeCallback.getQuotaResource();
    } catch (QuotaException quotaException) {
      LOGGER.error(
          "Could create QuotaResource object during {} operation for the chunk {} due to {}. This should never happen.",
          operationName, blobId.toString(), quotaException.toString());
    }
    // A null return means quota resource could not be created for this chunk. The consumer should decide how to handle nulls.
    return null;
  }

  @Override
  public QuotaMethod getQuotaMethod() {
    return quotaChargeCallback.getQuotaMethod();
  }
}
