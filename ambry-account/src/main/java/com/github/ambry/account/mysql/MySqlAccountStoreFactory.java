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
package com.github.ambry.account.mysql;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.mysql.MySqlUtils.*;


/**
 * Factory class to return an instance of {@link MySqlAccountStore} on {@link #getMySqlAccountStore()} call
 */
public class MySqlAccountStoreFactory {

  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountStoreFactory.class);
  private static final String UNKNOWN_DATACENTER = "Unknown";

  protected final MySqlAccountServiceConfig accountServiceConfig;
  protected final String localDatacenter;
  protected final MySqlAccountStoreMetrics metrics;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link MySqlAccountStore} instance. Cannot be {@code null}.
   */
  public MySqlAccountStoreFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.accountServiceConfig = new MySqlAccountServiceConfig(verifiableProperties);
    // If datacenter is unknown, all endpoints will be considered remote
    this.localDatacenter =
        verifiableProperties.getString(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, UNKNOWN_DATACENTER);
    this.metrics = new MySqlAccountStoreMetrics(metricRegistry);
  }

  /**
   * Returns an instance of the {@link MySqlAccountStore} that the factory generates.
   * @return an instance of {@link MySqlAccountStore} generated by this factory.
   * @throws SQLException
   */
  public MySqlAccountStore getMySqlAccountStore() throws SQLException {
    Map<String, List<MySqlUtils.DbEndpoint>> dcToMySqlDBEndpoints = getDbEndpointsPerDC(accountServiceConfig.dbInfo);
    // Flatten to List (TODO: does utility method need to return map?)
    List<MySqlUtils.DbEndpoint> dbEndpoints = new ArrayList<>();
    dcToMySqlDBEndpoints.values().forEach(endpointList -> dbEndpoints.addAll(endpointList));
    try {
      return new MySqlAccountStore(dbEndpoints, localDatacenter, metrics);
    } catch (SQLException e) {
      logger.error("MySQL account store creation failed", e);
      throw e;
    }
  }
}