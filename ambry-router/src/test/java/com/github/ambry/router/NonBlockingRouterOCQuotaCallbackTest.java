package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.quota.AmbryQuotaManager;
import com.github.ambry.quota.MaxThrottlePolicy;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.ThrottlePolicy;
import com.github.ambry.quota.ThrottlingRecommendation;
import com.github.ambry.quota.capacityunit.JsonCUQuotaEnforcer;
import com.github.ambry.quota.capacityunit.JsonCUQuotaSource;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class NonBlockingRouterOCQuotaCallbackTest extends NonBlockingRouterTestBase {
  private static final Logger logger = LoggerFactory.getLogger(NonBlockingRouterQuotaCallbackTest.class);

  private final QuotaMode throttlingMode;
  private final ChargeTesterQuotaManager quotaManager;
  private final JsonCUQuotaSource quotaSource;
  private final JsonCUQuotaEnforcer quotaEnforcer;
  private final QuotaConfig quotaConfig;
  private final boolean throttleInProgressRequests;
  private final long quotaAccountingSize = 1024L;

  /**
   * Initialize parameters common to all tests.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise.
   * @param quotaModeStr {@link QuotaMode} for router.
   * @param throttleInProgressRequests {@code true} if in progress request can be throttled. {@code false} otherwise.
   * @throws Exception if initialization fails.
   */
  public NonBlockingRouterOCQuotaCallbackTest(boolean testEncryption, String quotaModeStr,
      boolean throttleInProgressRequests) throws Exception {
    super(testEncryption, MessageFormatRecord.Metadata_Content_Version_V3, false);
    this.throttlingMode = QuotaMode.valueOf(quotaModeStr);
    this.throttleInProgressRequests = throttleInProgressRequests;
    // TODO test for throttling mode disabled.
    Properties quotaProperties = new Properties();
    quotaProperties.setProperty("quota.charge.quota.pre.process", "true");
    quotaProperties.setProperty("quota.throttling.mode", throttlingMode.name());
    quotaConfig = new QuotaConfig(new VerifiableProperties(quotaProperties));
    quotaManager = new NonBlockingRouterOCQuotaCallbackTest.ChargeTesterQuotaManager(quotaConfig,
        new MaxThrottlePolicy(quotaConfig), accountService, null, new MetricRegistry());
    quotaEnforcer = quotaManager.getQuotaEnforcer();
    quotaSource = (JsonCUQuotaSource) quotaEnforcer.getQuotaSource();
  }

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{false, QuotaMode.THROTTLING.name(), true}, {true, QuotaMode.THROTTLING.name(), true},
            {false, QuotaMode.TRACKING.name(), true}, {true, QuotaMode.TRACKING.name(), true},
            {false, QuotaMode.THROTTLING.name(), false}, {true, QuotaMode.THROTTLING.name(), false},
            {false, QuotaMode.TRACKING.name(), false}, {true, QuotaMode.TRACKING.name(), false}});
  }

  /**
   * Test default {@link QuotaChargeCallback} doesn't charge anything and doesn't error out when throttling is disabled.
   */
  @Test
  public void testRouterWithSufficientResourceQuota() throws Exception {
    try {
      Properties routerProperties = new Properties();
      routerProperties.setProperty("router.operation.controller",
          "com.github.ambry.router.TestQuotaAwareOperationController");
      routerProperties.setProperty("router.max.in.mem.put.chunks", "2");
      try {
        setRouter(routerProperties);
      } catch (ReflectiveOperationException reflectiveOperationException) {
        logger.info(reflectiveOperationException.toString());
      }
      Account account = createAccountWithQuota(1024, 1024);
      assertExpectedThreadCounts(2, 1);
      QuotaChargeCallback quotaChargeCallback =
          new TestQuotaChargeCallback(createRestRequest(RestMethod.POST.name(), "/", account),
              quotaManager, true);

      int blobSize = 3000;
      setOperationParams(blobSize, TTL_SECS);
      String compositeBlobId =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT, null,
              quotaChargeCallback).get();
      int expectedCU =
          (int) (blobSize / quotaConfig.quotaAccountingUnit) + (((blobSize % quotaConfig.quotaAccountingUnit) == 0) ? 0
              : 1) + 1;
      assertEquals(expectedCU,
          (long) quotaSource.getUsage(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
              QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(expectedCU, (long) quotaSource.getFeUsage(QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(0, (long) quotaSource.getFeUsage(QuotaName.READ_CAPACITY_UNIT).getQuotaValue());

      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      quotaChargeCallback = new TestQuotaChargeCallback(
          createRestRequest(RestMethod.GET.name(), "/" + compositeBlobId, account), quotaManager, true);
      router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback)
          .get()
          .getBlobDataChannel()
          .readInto(retainingAsyncWritableChannel, null)
          .get();
      // read out all the chunks.
      retainingAsyncWritableChannel.consumeContentAsInputStream().close();
      int newExpectedCU = expectedCU * 2;
      assertEquals(expectedCU,
          (long) quotaSource.getUsage(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
              QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
      assertEquals(expectedCU, (long) quotaSource.getFeUsage(QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
      assertEquals(expectedCU, (long) quotaSource.getFeUsage(QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    } finally {
      router.close();
      assertExpectedThreadCounts(0, 0);

      //submission after closing should return a future that is already done.
      assertClosed();
    }
  }

  @Test
  public void testRouterWithInSufficientResourceQuota() throws Exception {
    try {
      Properties routerProperties = new Properties();
      routerProperties.setProperty("router.operation.controller",
          "com.github.ambry.router.TestQuotaAwareOperationController");
      routerProperties.setProperty("router.max.in.mem.put.chunks", "2");
      try {
        setRouter(routerProperties);
      } catch (ReflectiveOperationException reflectiveOperationException) {
        logger.info(reflectiveOperationException.toString());
      }
      Account account = createAccountWithQuota(2, 2);
      assertExpectedThreadCounts(2, 1);
      QuotaChargeCallback quotaChargeCallback =
          new TestQuotaChargeCallback(createRestRequest(RestMethod.POST.name(), "/", account),
              quotaManager, true);

      int blobSize = 5000;
      setOperationParams(blobSize, TTL_SECS);
      Future<String> putBlobResult =
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT, null,
              quotaChargeCallback);
      Thread.sleep(2000);
      int expectedCU = 6;
      assertEquals(2,
          (long) quotaSource.getUsage(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
              QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(2, (long) quotaSource.getFeUsage(QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(0, (long) quotaSource.getFeUsage(QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
      assertFalse(putBlobResult.isDone());

      assertEquals(0, ((QuotaAwareOperationController)router.getAllOperationControllers().get(0)).getRequestQueue(QuotaMethod.READ).size());
      assertEquals(2, ((QuotaAwareOperationController)router.getAllOperationControllers().get(0)).getRequestQueue(QuotaMethod.WRITE).size());

      updateQuota(account,2, 2);
      Thread.sleep(2000);
      assertEquals(4,
          (long) quotaSource.getUsage(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
              QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(4, (long) quotaSource.getFeUsage(QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(0, (long) quotaSource.getFeUsage(QuotaName.READ_CAPACITY_UNIT).getQuotaValue());

      assertEquals(0, ((QuotaAwareOperationController)router.getAllOperationControllers().get(0)).getRequestQueue(QuotaMethod.READ).size());
      assertEquals(1, ((QuotaAwareOperationController)router.getAllOperationControllers().get(0)).getRequestQueue(QuotaMethod.WRITE).size());

      updateQuota(account,2, 2);
      Thread.sleep(2000);
      assertEquals(6,
          (long) quotaSource.getUsage(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
              QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(6, (long) quotaSource.getFeUsage(QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
      assertEquals(0, (long) quotaSource.getFeUsage(QuotaName.READ_CAPACITY_UNIT).getQuotaValue());

      assertEquals(0, ((QuotaAwareOperationController)router.getAllOperationControllers().get(0)).getRequestQueue(QuotaMethod.READ).size());
      assertEquals(0, ((QuotaAwareOperationController)router.getAllOperationControllers().get(0)).getRequestQueue(QuotaMethod.WRITE).size());
      assertTrue(putBlobResult.isDone());
      String compositeBlobId = putBlobResult.get();
      RetainingAsyncWritableChannel retainingAsyncWritableChannel = new RetainingAsyncWritableChannel();
      quotaChargeCallback = new TestQuotaChargeCallback(
          createRestRequest(RestMethod.GET.name(), "/" + compositeBlobId, account), quotaManager, true);
      router.getBlob(compositeBlobId, new GetBlobOptionsBuilder().build(), null, quotaChargeCallback)
          .get()
          .getBlobDataChannel()
          .readInto(retainingAsyncWritableChannel, null)
          .get();
      // read out all the chunks.
      retainingAsyncWritableChannel.consumeContentAsInputStream().close();
      int newExpectedCU = expectedCU * 2;
      assertEquals(expectedCU,
          (long) quotaSource.getUsage(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
              QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
      assertEquals(expectedCU, (long) quotaSource.getFeUsage(QuotaName.READ_CAPACITY_UNIT).getQuotaValue());
      assertEquals(expectedCU, (long) quotaSource.getFeUsage(QuotaName.WRITE_CAPACITY_UNIT).getQuotaValue());
    } finally {
      router.close();
      assertExpectedThreadCounts(0, 0);

      //submission after closing should return a future that is already done.
      assertClosed();
    }
  }

  private Account createAccountWithQuota(long rcu, long wcu) {
    Account account = accountService.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
    updateQuota(account, rcu, wcu);
    return account;
  }

  private void updateQuota( Account account, long rcu, long wcu) {
    quotaSource.updateQuota(new QuotaResource(String.valueOf(account.getId()), QuotaResourceType.ACCOUNT),
        new JsonCUQuotaSource.CUQuota(rcu, wcu));
  }

  private MockRestRequest createRestRequest(String restMethodName, String requestPath, Account account)
      throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, restMethodName);
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse(requestPath, Collections.EMPTY_MAP, null, ""));
    headers.put(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
    headers.put(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, account.getAllContainers().iterator().next());
    data.put(MockRestRequest.HEADERS_KEY, headers);
    data.put(MockRestRequest.URI_KEY, requestPath);
    return new MockRestRequest(data, null);
  }

  /**
   * {@link AmbryQuotaManager} extension to test behavior with default implementation.
   */
  static class ChargeTesterQuotaManager extends AmbryQuotaManager {
    private final AtomicInteger chargeCalledCount = new AtomicInteger(0);
    private final AtomicInteger chargeIfUsageWithinQuotaCalledCount = new AtomicInteger(0);
    private final AtomicInteger chargeIfQuotaExceedAllowedCount = new AtomicInteger(0);

    /**
     * Constructor for {@link NonBlockingRouterQuotaCallbackTest.ChargeTesterQuotaManager}.
     * @param quotaConfig {@link QuotaConfig} object.
     * @param throttlePolicy {@link ThrottlePolicy} object that makes the overall recommendation.
     * @param accountService {@link AccountService} object to get all the accounts and container information.
     * @param accountStatsStore {@link AccountStatsStore} object to get all the account stats related information.
     * @param metricRegistry {@link MetricRegistry} object for creating quota metrics.
     * @throws ReflectiveOperationException in case of any exception.
     */
    public ChargeTesterQuotaManager(QuotaConfig quotaConfig, ThrottlePolicy throttlePolicy,
        AccountService accountService, AccountStatsStore accountStatsStore, MetricRegistry metricRegistry)
        throws ReflectiveOperationException {
      super(quotaConfig, throttlePolicy, accountService, accountStatsStore, metricRegistry);
    }

    @Override
    public ThrottlingRecommendation charge(RestRequest restRequest, BlobInfo blobInfo,
        Map<QuotaName, Double> requestCostMap) {
      chargeCalledCount.incrementAndGet();
      ThrottlingRecommendation throttlingRecommendation = super.charge(restRequest, blobInfo, requestCostMap);
      return throttlingRecommendation;
    }

    @Override
    public boolean chargeIfUsageWithinQuota(RestRequest restRequest, BlobInfo blobInfo,
        Map<QuotaName, Double> requestCostMap) throws QuotaException {
      chargeIfUsageWithinQuotaCalledCount.incrementAndGet();
      boolean throttlingRecommendation = super.chargeIfUsageWithinQuota(restRequest, blobInfo, requestCostMap);
      return throttlingRecommendation;
    }

    @Override
    public boolean chargeIfQuotaExceedAllowed(RestRequest restRequest, BlobInfo blobInfo,
        Map<QuotaName, Double> requestCostMap) throws QuotaException {
      chargeIfQuotaExceedAllowedCount.incrementAndGet();
      return super.chargeIfQuotaExceedAllowed(restRequest, blobInfo, requestCostMap);
    }

    public JsonCUQuotaEnforcer getQuotaEnforcer() {
      return (JsonCUQuotaEnforcer) quotaEnforcers.iterator().next();
    }

    public int getChargeCalledCount() {
      return chargeCalledCount.get();
    }

    public int getChargeIfUsageWithinQuotaCalledCount() {
      return chargeIfUsageWithinQuotaCalledCount.get();
    }

    public int getChargeIfQuotaExceedAllowedCount() {
      return chargeIfQuotaExceedAllowedCount.get();
    }
  }

  static class TestQuotaChargeCallback implements QuotaChargeCallback {
    private final QuotaChargeCallback quotaChargeCallback;
    private final AtomicInteger checkAndChargeCallCount = new AtomicInteger(0);
    private final AtomicInteger checkAndChargeSzCallCount = new AtomicInteger(0);
    private final AtomicInteger checkCallCount = new AtomicInteger(0);
    private final AtomicInteger chargeIfQuotaExceedAllowedCallCount = new AtomicInteger(0);
    private final AtomicInteger chargeIfQuotaExceedAllowedSzCallCount = new AtomicInteger(0);

    public TestQuotaChargeCallback(RestRequest restRequest, QuotaManager quotaManager, boolean shouldThrottle) {
      quotaChargeCallback = QuotaChargeCallback.buildQuotaChargeCallback(restRequest, quotaManager, shouldThrottle);
    }

    @Override
    public boolean checkAndCharge(long chunkSize) throws QuotaException {
      checkAndChargeSzCallCount.incrementAndGet();
      return quotaChargeCallback.checkAndCharge(chunkSize);
    }

    @Override
    public boolean checkAndCharge() throws QuotaException {
      checkAndChargeCallCount.incrementAndGet();
      return quotaChargeCallback.checkAndCharge();
    }

    @Override
    public boolean check() throws QuotaException {
      checkCallCount.incrementAndGet();
      return quotaChargeCallback.check();
    }

    @Override
    public boolean chargeIfQuotaExceedAllowed() throws QuotaException {
      chargeIfQuotaExceedAllowedCallCount.incrementAndGet();
      return quotaChargeCallback.chargeIfQuotaExceedAllowed();
    }

    @Override
    public boolean chargeIfQuotaExceedAllowed(long chunkSize) throws QuotaException {
      chargeIfQuotaExceedAllowedSzCallCount.incrementAndGet();
      return quotaChargeCallback.chargeIfQuotaExceedAllowed(chunkSize);
    }

    @Override
    public QuotaResource getQuotaResource() throws QuotaException {
      return quotaChargeCallback.getQuotaResource();
    }

    @Override
    public QuotaMethod getQuotaMethod() {
      return quotaChargeCallback.getQuotaMethod();
    }

    @Override
    public QuotaConfig getQuotaConfig() {
      return quotaChargeCallback.getQuotaConfig();
    }
  }
}
