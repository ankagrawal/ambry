package com.github.ambry.frontend;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettySslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.AmbryQuotaManager;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSource;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.ByteRanges;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.NonBlockingRouter;
import com.github.ambry.router.NonBlockingRouterTestBase;
import com.github.ambry.router.OperationController;
import com.github.ambry.router.QuotaAwareOperationController;
import com.github.ambry.utils.TestUtils;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.utils.TestUtils.*;

@RunWith(Parameterized.class)
public class FrontendPreProcessQuotaIntegrationTest extends FrontendIntegrationTestBase {
  private static final String DEFAULT_PARTITION_CLASS = "default-partition-class";
  private static final MockClusterMap CLUSTER_MAP;
  private static final VerifiableProperties FRONTEND_VERIFIABLE_PROPS;
  private static final File TRUST_STORE_FILE;
  private static final FrontendConfig FRONTEND_CONFIG;
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static Account ACCOUNT;
  private static Container CONTAINER;
  private static long DEFAULT_ACCEPT_QUOTA = 1024;
  private static long DEFAULT_REJECT_QUOTA = 0;
  private static RestServer ambryRestServer = null;
  private static QuotaAwareOperationController quotaAwareOperationController;
  private static AmbryCUQuotaSource ambryCUQuotaSource;
  private final boolean throttleRequest;
  private final QuotaMode quotaMode;

  /**
   * @param throttleRequest {@code true} if quota manager should reject quota requests.
   */
  public FrontendPreProcessQuotaIntegrationTest(boolean throttleRequest, QuotaMode quotaMode) {
    super(null, null);
    this.throttleRequest = throttleRequest;
    this.quotaMode = quotaMode;
  }

  /**
   * @return a list of arrays that represent the constructor argument to make quota manager accept or reject requests.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{true, QuotaMode.TRACKING}, {false, QuotaMode.TRACKING}, {true, QuotaMode.THROTTLING},
            {false, QuotaMode.THROTTLING}});
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @param isRequestQuotaEnabled flag to specify if request quota is enabled.
   * @param quotaMode {@link QuotaMode} object.
   * @param account {@link Account} for which quota needs to be specified.
   * @param throttleRequest flag to indicate if the {@link com.github.ambry.quota.QuotaManager} should throttle request.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVPropsForQuota(File trustStoreFile,
      boolean isRequestQuotaEnabled, QuotaMode quotaMode, Account account, boolean throttleRequest) throws IOException,
                                                                                                           GeneralSecurityException {
    Properties properties = buildFrontendVProps(trustStoreFile, true, PLAINTEXT_SERVER_PORT, SSL_SERVER_PORT);
    // By default the usage and limit of quota will be 0 in the default JsonCUQuotaSource, and hence the default
    // JsonCUQuotaEnforcer will reject requests. So for cases where we don't want requests to be rejected, we set a
    // non 0 limit for quota.
    JSONObject cuResourceQuotaJson = new JSONObject();
    JSONObject quotaJson = new JSONObject();
    quotaJson.put("rcu", throttleRequest ? 0 : 10737418240L);
    quotaJson.put("wcu", throttleRequest ? 0: 10737418240L);
    cuResourceQuotaJson.put(Integer.toString(account.getId()), quotaJson);
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON, cuResourceQuotaJson.toString());
    properties.setProperty(QuotaConfig.THROTTLING_MODE, quotaMode.name());
    properties.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, String.valueOf(isRequestQuotaEnabled));
    properties.setProperty(QuotaConfig.FRONTEND_CU_CAPACITY_IN_JSON, "{\n" + "  \"rcu\": 1024,\n"
        + "  \"wcu\": 1024\n" + "}");
    long quotaValue = throttleRequest ? DEFAULT_REJECT_QUOTA : DEFAULT_ACCEPT_QUOTA;
    properties.setProperty(QuotaConfig.RESOURCE_CU_QUOTA_IN_JSON,
        String.format("{\n" + "  \"%s\": {\n" + "    \"rcu\": %d,\n" + "    \"wcu\": %d\n" + "  }\n" + "}", account.getId(), quotaValue, quotaValue));
    return new VerifiableProperties(properties);
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile)
      throws IOException, GeneralSecurityException {
    return new VerifiableProperties(buildFrontendVProps(trustStoreFile, true, PLAINTEXT_SERVER_PORT, SSL_SERVER_PORT));
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @param enableUndelete enable undelete in frontend when it's true.
   * @param plaintextServerPort server port number to support plaintext protocol
   * @param sslServerPort server port number to support ssl protocol
   * @return a {@link Properties} with the parameters for an Ambry frontend server.
   */
  private static Properties buildFrontendVProps(File trustStoreFile, boolean enableUndelete, int plaintextServerPort,
      int sslServerPort) throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.put("rest.server.rest.request.service.factory",
        "com.github.ambry.frontend.FrontendRestRequestServiceFactory");
    properties.put("rest.server.router.factory", "com.github.ambry.router.InMemoryRouterFactory");
    properties.put("rest.server.account.service.factory", "com.github.ambry.account.InMemAccountServiceFactory");
    properties.put("netty.server.port", Integer.toString(plaintextServerPort));
    properties.put("netty.server.ssl.port", Integer.toString(sslServerPort));
    properties.put("netty.server.enable.ssl", "true");
    properties.put(NettyConfig.SSL_FACTORY_KEY, NettySslFactory.class.getName());
    // to test that backpressure does not impede correct operation.
    properties.put("netty.server.request.buffer.watermark", "1");
    // to test that multipart requests over a certain size fail
    properties.put("netty.multipart.post.max.size.bytes", Long.toString(MAX_MULTIPART_POST_SIZE_BYTES));
    CommonTestUtils.populateRequiredRouterProps(properties);
    TestSSLUtils.addSSLProperties(properties, "", SSLFactory.Mode.SERVER, trustStoreFile, "frontend");
    // add key for singleKeyManagementService
    properties.put("kms.default.container.key", TestUtils.getRandomKey(32));
    properties.setProperty("clustermap.cluster.name", CLUSTER_NAME);
    properties.setProperty("clustermap.datacenter.name", DATA_CENTER_NAME);
    properties.setProperty("clustermap.host.name", HOST_NAME);
    properties.setProperty("clustermap.port", String.valueOf(PORT));
    properties.setProperty(FrontendConfig.ENABLE_UNDELETE, Boolean.toString(enableUndelete));
    return properties;
  }

  /**
   * Sets up an Ambry frontend server.
   * @throws Exception
   */
  @Before
  public void setup() throws Exception {
    ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount(QuotaResourceType.ACCOUNT);
    CONTAINER = ACCOUNT.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID);
    VerifiableProperties quotaProps =
        buildFrontendVPropsForQuota(TRUST_STORE_FILE, true, quotaMode, ACCOUNT, throttleRequest);
    ambryRestServer = new RestServer(quotaProps, CLUSTER_MAP, new LoggingNotificationSystem(),
        SSLFactory.getNewInstance(new SSLConfig(FRONTEND_VERIFIABLE_PROPS)));
    Field restRequestServiceField = RestServer.class.getDeclaredField("restRequestService");
    restRequestServiceField.setAccessible(true);
    FrontendRestRequestService frontendRestRequestService = (FrontendRestRequestService) restRequestServiceField.get(ambryRestServer);
    Field quotaManagerField = FrontendRestRequestService.class.getDeclaredField("quotaManager");
    quotaManagerField.setAccessible(true);
    AmbryQuotaManager quotaManager = (AmbryQuotaManager) quotaManagerField.get(frontendRestRequestService);
    Field[]  fields = AmbryQuotaManager.class.getDeclaredFields();
    //Field quotaEnforcersField = AmbryQuotaManager.class.getField("quotaConfig");
    fields[1].setAccessible(true);
    Set<QuotaEnforcer> quotaEnforcerSet = (Set<QuotaEnforcer>) fields[1].get(quotaManager);
    List<QuotaSource> quotaSources = quotaEnforcerSet.stream().map(quotaEnforcer -> quotaEnforcer.getQuotaSource()).collect(
        Collectors.toList());
    for(QuotaSource quotaSource : quotaSources) {
      if(quotaSource instanceof AmbryCUQuotaSource) {
        ambryCUQuotaSource = (AmbryCUQuotaSource) quotaSource;
      } else {
        throw new IllegalStateException("Could not get AmbryCUQuotaSource instance.");
      }
    }

    Field routerField = RestServer.class.getDeclaredField("router");
    routerField.setAccessible(true);
    InMemoryRouter nonBlockingRouter = (InMemoryRouter) routerField.get(ambryRestServer);

    Field operationControllersField = NonBlockingRouter.class.getField("operationControllers");
    operationControllersField.setAccessible(true);
    ArrayList<OperationController> operationControllers = (ArrayList<OperationController>) operationControllersField.get(nonBlockingRouter);
    for(OperationController operationController : operationControllers) {
      if(operationController instanceof QuotaAwareOperationController) {
        quotaAwareOperationController = (QuotaAwareOperationController) operationController;
      } else {
        throw new IllegalStateException("Could not get operation controller instance.");
      }
    }
    ambryRestServer.start();
    this.frontendConfig = FRONTEND_CONFIG;
    this.nettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
  }

  /**
   * Shuts down the Ambry frontend server.
   */
  @After
  public void teardown() {
    if (nettyClient != null) {
      nettyClient.close();
    }
    if (ambryRestServer != null) {
      ambryRestServer.shutdown();
    }
  }

  static {
    try {
      TRUST_STORE_FILE = File.createTempFile("truststore", ".jks");
      CLUSTER_MAP = new MockClusterMap();
      TRUST_STORE_FILE.deleteOnExit();
      FRONTEND_VERIFIABLE_PROPS = buildFrontendVProps(TRUST_STORE_FILE);
      FRONTEND_CONFIG = new FrontendConfig(FRONTEND_VERIFIABLE_PROPS);
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Tests blob POST, GET, HEAD, TTL update and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadUpdateDeleteUndeleteTest() throws Exception {
    int refContentSize = (int) FRONTEND_CONFIG.chunkedGetResponseThresholdInBytes * 3;
    doPostGetHeadUpdateDeleteUndeleteTest(refContentSize, ACCOUNT, CONTAINER, ACCOUNT.getName(),
        !CONTAINER.isCacheable(), ACCOUNT.getName(), CONTAINER.getName(), false);
  }

  /**
   * Utility to test blob POST, GET, HEAD and DELETE operations for a specified size
   * @param contentSize the size of the blob to be tested
   * @param toPostAccount the {@link Account} to use in post headers. Can be {@code null} if only using service ID.
   * @param toPostContainer the {@link Container} to use in post headers. Can be {@code null} if only using service ID.
   * @param serviceId the serviceId to use for the POST
   * @param isPrivate the isPrivate flag to pass as part of the POST
   * @param expectedAccountName the expected account name in some response.
   * @param expectedContainerName the expected container name in some responses.
   * @param multipartPost {@code true} if multipart POST is desired, {@code false} otherwise.
   * @throws Exception
   */
  void doPostGetHeadUpdateDeleteUndeleteTest(int contentSize, Account toPostAccount, Container toPostContainer,
      String serviceId, boolean isPrivate, String expectedAccountName, String expectedContainerName,
      boolean multipartPost) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(contentSize));
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    String accountNameInPost = toPostAccount != null ? toPostAccount.getName() : null;
    String containerNameInPost = toPostContainer != null ? toPostContainer.getName() : null;
    HttpHeaders headers = new DefaultHttpHeaders();
    setAmbryHeadersForPut(headers, TTL_SECS, isPrivate, serviceId, contentType, ownerId, accountNameInPost,
        containerNameInPost);
    byte[] usermetadata = null;

    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    headers.add(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    String blobId = postBlobAndVerify(headers, content, contentSize);
    Assert.assertEquals(8192*3/(1024), ambryCUQuotaSource.getAllQuotaUsage().get(QuotaResource.fromAccount(ACCOUNT).getResourceId()));
    /*
    headers.add(RestUtils.Headers.BLOB_SIZE, content.capacity());
    headers.add(RestUtils.Headers.LIFE_VERSION, "0");
    getBlobAndVerify(blobId, null, null, false, headers, isPrivate, content, expectedAccountName,
        expectedContainerName);
    getHeadAndVerify(blobId, null, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    getBlobAndVerify(blobId, null, GetOption.None, false, headers, isPrivate, content, expectedAccountName,
        expectedContainerName);
    getHeadAndVerify(blobId, null, GetOption.None, headers, isPrivate, expectedAccountName, expectedContainerName);
    ByteRange range = ByteRanges.fromLastNBytes(ThreadLocalRandom.current().nextLong(content.capacity() + 1));
    headers.add(RestUtils.Headers.BLOB_SIZE, range.getRangeSize());
    getBlobAndVerify(blobId, range, null, false, headers, isPrivate, content, expectedAccountName,
        expectedContainerName);
    getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    if (contentSize > 0) {
      range = ByteRanges.fromStartOffset(ThreadLocalRandom.current().nextLong(content.capacity()));
      getBlobAndVerify(blobId, range, null, false, headers, isPrivate, content, expectedAccountName,
          expectedContainerName);
      getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
      long random1 = ThreadLocalRandom.current().nextLong(content.capacity());
      long random2 = ThreadLocalRandom.current().nextLong(content.capacity());
      range = ByteRanges.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2));
      getBlobAndVerify(blobId, range, null, false, headers, isPrivate, content, expectedAccountName,
          expectedContainerName);
      getHeadAndVerify(blobId, range, null, headers, isPrivate, expectedAccountName, expectedContainerName);
    }
    getNotModifiedBlobAndVerify(blobId, null, isPrivate);
    getUserMetadataAndVerify(blobId, null, headers, usermetadata);
    getBlobInfoAndVerify(blobId, null, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata);
    updateBlobTtlAndVerify(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD, TTL update and DELETE after delete.
    verifyOperationsAfterDelete(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, content,
        usermetadata);
    // Undelete it
    headers.add(RestUtils.Headers.LIFE_VERSION, "1");
    undeleteBlobAndVerify(blobId, headers, isPrivate, expectedAccountName, expectedContainerName, usermetadata);
    */
  }

  protected Properties getNonBlockingRouterProperties(String routerDataCenter, int putParallelism,
      int deleteParallelism) {
    Properties properties = new Properties();
    properties.setProperty("rest.server.router.factory", TestNonBlockingRouterFactory.class.getCanonicalName());
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", CLUSTER_MAP.getDatacenterName((byte) 0));
    properties.setProperty("router.connections.local.dc.warm.up.percentage", Integer.toString(67));
    properties.setProperty("router.connections.remote.dc.warm.up.percentage", Integer.toString(34));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", CLUSTER_MAP.getDatacenterName((byte) 0));
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(128));
    return properties;
  }
}
