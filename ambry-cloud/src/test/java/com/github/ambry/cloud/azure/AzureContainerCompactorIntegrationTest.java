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
package com.github.ambry.cloud.azure;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Work In Progress: Test for {@link AzureContainerCompactor}
 */
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class AzureContainerCompactorIntegrationTest {

  private final Random random = new Random();
  private final String PROPS_FILE_NAME = "azure-test.properties";
  private static AzureCloudConfig azureConfig;
  private static VerifiableProperties verifiableProperties;
  private static AzureCloudDestination cloudDestination;

  @Before
  public void setup() throws ReflectiveOperationException {
    // TODO Create the required cosmos table as well as the required azure blob.
    Properties testProperties = new Properties();
    try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(PROPS_FILE_NAME)) {
      if (input == null) {
        throw new IllegalStateException("Could not find resource: " + PROPS_FILE_NAME);
      }
      testProperties.load(input);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load properties from resource: " + PROPS_FILE_NAME);
    }
    testProperties.setProperty("clustermap.cluster.name", "Integration-Test");
    testProperties.setProperty("clustermap.datacenter.name", "uswest");
    testProperties.setProperty("clustermap.host.name", "localhost");
    testProperties.setProperty("kms.default.container.key",
        "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");

    testProperties.setProperty(CloudConfig.CLOUD_DELETED_BLOB_RETENTION_DAYS, String.valueOf(1));
    testProperties.setProperty(AzureCloudConfig.AZURE_PURGE_BATCH_SIZE, "10");
    verifiableProperties = new VerifiableProperties(testProperties);
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    azureConfig = new AzureCloudConfig(verifiableProperties);
    MetricRegistry registry = new MetricRegistry();
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, verifiableProperties, registry);
    cloudDestination = (AzureCloudDestination) cloudDestinationFactory.getCloudDestination();
  }

  @After
  public void destroy() throws IOException {
    // TODO destroy the abs blob and cosmos db
    if (cloudDestination != null) {
      cloudDestination.close();
    }
  }

  @Test
  public void testUpdateDeletedContainer() throws CloudStorageException {
    Set<Container> containers = generateContainers(5);
    cloudDestination.deprecateContainers(containers, null);
    verifyCosmosData(containers);
    verifyCheckpoint(containers);
  }

  private void verifyCosmosData(Set<Container> containers) {
    // TODO: verify the deleted container information in cosmos table is correct.
  }

  private void verifyCheckpoint(Set<Container> containers) {
    // TODO verify that the information in checkpoint is correct
  }

  /**
   * Generate specified number of {@link Container}s.
   * @param numContainers number of {@link Container}s to generate.
   * @return {@link Set} of {@link Container}s.
   */
  private Set<Container> generateContainers(int numContainers) {
    Set<Container> containers = new HashSet<>();
    Set<Short> containerIdSet = new HashSet<>();
    for (int j = 0; j < numContainers; j++) {
      short containerId = Utils.getRandomShort(random);
      if (!containerIdSet.add(containerId)) {
        j--;
        continue;
      }
      String containerName = UUID.randomUUID().toString();
      Container.ContainerStatus containerStatus =
          random.nextBoolean() ? Container.ContainerStatus.DELETE_IN_PROGRESS : Container.ContainerStatus.INACTIVE;
      String containerDescription = UUID.randomUUID().toString();
      boolean containerCaching = random.nextBoolean();
      boolean containerEncryption = random.nextBoolean();
      boolean containerPreviousEncryption = containerEncryption || random.nextBoolean();
      boolean mediaScanDisabled = random.nextBoolean();
      String replicationPolicy = TestUtils.getRandomString(10);
      boolean ttlRequired = random.nextBoolean();
      ContainerBuilder containerBuilder =
          new ContainerBuilder(containerId, containerName, containerStatus, containerDescription,
              (short) random.nextInt()).setEncrypted(containerEncryption)
              .setPreviouslyEncrypted(containerPreviousEncryption)
              .setCacheable(containerCaching)
              .setMediaScanDisabled(mediaScanDisabled)
              .setReplicationPolicy(replicationPolicy)
              .setTtlRequired(ttlRequired)
              .setDeleteTriggerTime(System.currentTimeMillis());
      containers.add(containerBuilder.build());
    }
    return containers;
  }
}
