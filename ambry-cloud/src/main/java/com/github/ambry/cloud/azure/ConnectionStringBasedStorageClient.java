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

import com.azure.core.http.HttpClient;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.util.Configuration;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.github.ambry.config.CloudConfig;


/**
 * {@link StorageClient} implementation based on connection string authentication.
 */
public class ConnectionStringBasedStorageClient extends StorageClient {

  /**
   * Constructor for {@link ConnectionStringBasedStorageClient}.
   * @param cloudConfig {@link CloudConfig} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param azureMetrics {@link AzureMetrics} object.
   */
  public ConnectionStringBasedStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig,
      AzureMetrics azureMetrics) {
    super(cloudConfig, azureCloudConfig, azureMetrics);
  }

  /**
   * Constructor for {@link ConnectionStringBasedStorageClient}.
   * @param cloudConfig {@link CloudConfig} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public ConnectionStringBasedStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig,
      AzureMetrics azureMetrics, AzureBlobLayoutStrategy blobLayoutStrategy,
      AzureCloudConfig.StorageAccountInfo storageAccountInfo) {
    super(cloudConfig, azureCloudConfig, azureMetrics, blobLayoutStrategy, storageAccountInfo);
  }

  /**
   * Constructor for {@link ConnectionStringBasedStorageClient} object for testing.
   * @param blobServiceAsyncClient {@link BlobServiceClient} object.
   * @param blobBatchAsyncClient {@link BlobBatchClient} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  public ConnectionStringBasedStorageClient(BlobServiceAsyncClient blobServiceAsyncClient,
      BlobBatchAsyncClient blobBatchAsyncClient, AzureMetrics azureMetrics, AzureBlobLayoutStrategy blobLayoutStrategy,
      AzureCloudConfig azureCloudConfig, AzureCloudConfig.StorageAccountInfo storageAccountInfo) {
    super(blobServiceAsyncClient, blobBatchAsyncClient, azureMetrics, blobLayoutStrategy, azureCloudConfig,
        storageAccountInfo);
  }

  @Override
  protected BlobServiceAsyncClient buildBlobServiceAsyncClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig) {
    return new BlobServiceClientBuilder().connectionString(
            storageAccountInfo() != null ? storageAccountInfo().getStorageConnectionString()
                : azureCloudConfig.azureStorageConnectionString)
        .httpClient(httpClient)
        .retryOptions(retryOptions)
        .configuration(configuration)
        .buildAsyncClient();
  }

  protected BlobServiceClient buildBlobServiceSyncClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig) {
    try {
      return new BlobServiceClientBuilder().connectionString(azureCloudConfig.azureStorageConnectionString)
          .httpClient(httpClient)
          .retryOptions(retryOptions)
          .configuration(configuration)
          .buildClient();
    } catch (Throwable e) {
      String err = String.format("Failed to build blob service client with connection string %s due to %s",
          azureCloudConfig.azureStorageConnectionString, e.getMessage());
      logger.error(err);
      throw e;
    }
  }

  @Override
  protected TableServiceClient buildTableServiceClient(HttpClient httpClient, Configuration configuration,
      RetryOptions retryOptions, AzureCloudConfig azureCloudConfig) {
    try {
      return new TableServiceClientBuilder().connectionString(azureCloudConfig.azureTableConnectionString)
          .httpClient(httpClient)
          .retryOptions(retryOptions)
          .configuration(configuration)
          .buildClient();
    } catch (Throwable e) {
      String err = String.format("Failed to build table service client with connection string %s due to %s",
          azureCloudConfig.azureTableConnectionString, e.getMessage());
      logger.error(err);
      throw e;
    }
  }

  /**
   * Validate that all the required configs for connection string based authentication are present.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  protected void validateTableServiceConfigs(AzureCloudConfig azureCloudConfig) {
    if (azureCloudConfig.azureTableConnectionString == null ||
        azureCloudConfig.azureTableConnectionString.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing Azure Table connection string config " + AzureCloudConfig.AZURE_TABLE_CONNECTION_STRING);
    }
  }

  /**
   * Validate that all the required configs for connection string based authentication are present.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  protected void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig) {
    if (storageAccountInfo() != null) {
      if (storageAccountInfo().getStorageConnectionString().isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Missing connection string config %s for the storage account %s ",
                AzureCloudConfig.AZURE_STORAGE_ACCOUNT_INFO_STORAGE_CONNECTION_STRING, storageAccountInfo().getName()));
      }
    } else if (azureCloudConfig.azureStorageConnectionString.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing connection string config " + AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING);
    }
  }

  @Override
  protected boolean handleExceptionAndHintRetry(BlobStorageException blobStorageException) {
    return false;
  }
}
