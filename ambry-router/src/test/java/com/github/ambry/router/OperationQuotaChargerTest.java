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

import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class OperationQuotaChargerTest {
  private final BlobId BLOBID = new BlobId((short) 1, BlobId.BlobIdType.NATIVE, (byte) 1, (short) 1, (short) 1,
      new Partition((short) 1, "", PartitionState.READ_ONLY, 1073741824L), false, BlobId.BlobDataType.DATACHUNK);
  private final QuotaResource QUOTA_RESOURCE = new QuotaResource("test", QuotaResourceType.ACCOUNT);

  @Test
  public void testCheckAndCharge() throws Exception {
    testCheckAndCharge(true);
    testCheckAndCharge(false);
  }

  @Test
  public void testGetQuotaResource() throws Exception {
    // getQuotaResource should return null if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // getQuotaResource should return what quotaChargeCallback.getQuotaResource returns.
    QuotaResource quotaResource = new QuotaResource("test", QuotaResourceType.ACCOUNT);
    Mockito.when(quotaChargeCallback.getQuotaResource()).thenReturn(quotaResource);
    Assert.assertEquals("getQuotaResource should return what quotaChargeCallback.getQuotaResource returns.",
        quotaResource, quotaChargeCallback.getQuotaResource());

    // getQuotaResource should return null if quotaChargeCallback throws exception.
    Mockito.when(quotaChargeCallback.getQuotaResource())
        .thenThrow(
            new QuotaException("", new RestServiceException("", RestServiceErrorCode.InternalServerError), false));
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());
  }

  private void testCheckAndCharge(boolean shouldCheckExceedAllowed) throws Exception {
    // checkAndCharge should return allow if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertEquals("checkAndCharge should return allow if quotaChargeCallback is null.", QuotaAction.ALLOW,
        operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    Mockito.when(quotaChargeCallback.getQuotaResource()).thenReturn(QUOTA_RESOURCE);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // checkAndCharge should return allow if quotaChargeCallback returns ALLOW.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.ALLOW);
    Assert.assertEquals("checkAndCharge should return allow if quotaChargeCallback returns true.", QuotaAction.ALLOW,
        operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");
    // checkAndCharge should return delay if quotaChargeCallback returns DELAY.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.DELAY);
    Assert.assertEquals("checkAndCharge should return delay if quotaChargeCallback returns delay.", QuotaAction.DELAY,
        operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    // checkAndCharge should return allow if quotaChargeCallback returns delay but isCharged is true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.ALLOW);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.DELAY);
    Assert.assertEquals(
        "checkAndCharge should return allow if quotaChargeCallback returns DELAY but isCharged is true.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");
    // checkAndCharge should return REJECT if quotaChargeCallback returns REJECT.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.REJECT);
    Assert.assertEquals(
        "checkAndCharge should return reject if quotaChargeCallback returns DELAY but isCharged is true.",
        QuotaAction.REJECT, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    // checkAndCharge should return allow if quotaChargeCallback returns REJECT but isCharged is true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.ALLOW);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.checkAndCharge(false, false)).thenReturn(QuotaAction.REJECT);
    Assert.assertEquals(
        "checkAndCharge should return allow if quotaChargeCallback returns REJECT but isCharged is true.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");
    // test if any exception is thrown in quota charge callback.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false))
        .thenThrow(new QuotaException("test", false));
    // exception should not propagate.
    Assert.assertEquals("checkAndCharge should return allow if there is any exception from quotachargecallback.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    Mockito.doReturn(QuotaAction.ALLOW).when(quotaChargeCallback).checkAndCharge(shouldCheckExceedAllowed, false);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false))
        .thenThrow(new QuotaException("test", false));
    Assert.assertEquals("checkAndCharge should return allow if there is any exception from quotachargecallback.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));
  }
}
