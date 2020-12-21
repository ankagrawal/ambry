/**
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
package com.github.ambry.frontend;

import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.NettySslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.AmbryQuotaManagerFactory;
import com.github.ambry.quota.QuotaManagerFactory;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.quota.RejectHostQuotaEnforcerFactory;
import com.github.ambry.rest.NettyClient;
import com.github.ambry.rest.RestServer;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.junit.Test;


/**
 * Test for host resource throttling.
 */
public class HostThrottlerTest {
  private static final int PLAINTEXT_SERVER_PORT = 1174;
  private static final int SSL_SERVER_PORT = 1175;
  private static final int MAX_MULTIPART_POST_SIZE_BYTES = 10 * 10 * 1024;
  private static final MockClusterMap CLUSTER_MAP;
  private static final VerifiableProperties SSL_CLIENT_VERIFIABLE_PROPS;
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final String DATA_CENTER_NAME = "Datacenter-Name";
  private static final String HOST_NAME = "localhost";
  private static final String CLUSTER_NAME = "Cluster-name";
  private static final File trustFile;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
      trustFile = File.createTempFile("truststore", ".jks");
      trustFile.deleteOnExit();
      SSL_CLIENT_VERIFIABLE_PROPS = TestSSLUtils.createSslProps("", SSLFactory.Mode.CLIENT, trustFile, "client");
      ACCOUNT_SERVICE.clear();
      ACCOUNT_SERVICE.updateAccounts(Collections.singletonList(InMemAccountService.UNKNOWN_ACCOUNT));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Builds properties required to start a {@link RestServer} as an Ambry frontend server.
   * @param trustStoreFile the trust store file to add certificates to for SSL testing.
   * @param plaintextServerPort server port number to support plaintext protocol
   * @param sslServerPort server port number to support ssl protocol
   * @param quotaMode {@link QuotaMode} object.
   * @param isHostThrottlingEnabled true is host throttling is enabled. false otherwise.
   * @throws IOException
   * @throws GeneralSecurityException
   * @return a {@link VerifiableProperties} with the parameters for an Ambry frontend server.
   */
  private static VerifiableProperties buildFrontendVProps(File trustStoreFile, int plaintextServerPort,
      int sslServerPort, QuotaMode quotaMode, boolean isHostThrottlingEnabled)
      throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    properties.setProperty(QuotaConfig.QUOTA_THROTTLING_MODE, quotaMode.name());
    properties.setProperty(QuotaConfig.HOST_QUOTA_THROTTLING_ENABLED, String.valueOf(isHostThrottlingEnabled));
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
    return new VerifiableProperties(properties);
  }

  @Test
  public void hostThrottlingTest() throws Exception {
    NettyClient plaintextNettyClient = null;
    NettyClient sslNettyClient = null;
    RestServer ambryRestServer = null;
    try {
      QuotaConfig quotaConfig =
          QuotaTestUtils.createQuotaConfig(Collections.singletonList(RejectHostQuotaEnforcerFactory.class.getName()),
              true, QuotaMode.THROTTLING, true);
      QuotaManagerFactory quotaManagerFactory =
          new AmbryQuotaManagerFactory(quotaConfig, Collections.emptyList(), Collections.emptyList());
      VerifiableProperties verifiableProperties =
          buildFrontendVProps(trustFile, PLAINTEXT_SERVER_PORT, SSL_SERVER_PORT, QuotaMode.THROTTLING, true);
      ambryRestServer = new RestServer(verifiableProperties, CLUSTER_MAP, new LoggingNotificationSystem(),
          SSLFactory.getNewInstance(new SSLConfig(verifiableProperties)), ACCOUNT_SERVICE, quotaManagerFactory);
      ambryRestServer.start();
      plaintextNettyClient = new NettyClient("localhost", PLAINTEXT_SERVER_PORT, null);
      sslNettyClient = new NettyClient("localhost", SSL_SERVER_PORT,
          SSLFactory.getNewInstance(new SSLConfig(SSL_CLIENT_VERIFIABLE_PROPS)));
      FullHttpRequest httpRequest =
          buildRequest(HttpMethod.GET, UUID.randomUUID().toString(), new DefaultHttpHeaders(), null);
      plaintextNettyClient.sendRequest(httpRequest, null, null).get();
    } finally {
      if (plaintextNettyClient != null) {
        plaintextNettyClient.close();
      }
      if (sslNettyClient != null) {
        sslNettyClient.close();
      }
      if (ambryRestServer != null) {
        ambryRestServer.shutdown();
      }
    }
  }

  /**
   * Method to easily create a request.
   * @param httpMethod the {@link HttpMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link HttpHeaders} object. Can be null.
   * @param content the content that accompanies the request. Can be null.
   * @return A {@link FullHttpRequest} object that defines the request required by the input.
   */
  private FullHttpRequest buildRequest(HttpMethod httpMethod, String uri, HttpHeaders headers, ByteBuffer content) {
    ByteBuf contentBuf;
    if (content != null) {
      contentBuf = Unpooled.wrappedBuffer(content);
    } else {
      contentBuf = Unpooled.buffer(0);
    }
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, uri, contentBuf);
    if (headers != null) {
      httpRequest.headers().set(headers);
    }
    if (HttpMethod.POST.equals(httpMethod) && !HttpUtil.isContentLengthSet(httpRequest)) {
      HttpUtil.setTransferEncodingChunked(httpRequest, true);
    }
    return httpRequest;
  }
}