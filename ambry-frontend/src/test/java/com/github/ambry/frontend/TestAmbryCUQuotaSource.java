package com.github.ambry.frontend;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSource;
import com.github.ambry.quota.capacityunit.CapacityUnit;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;


/**
 *
 */
public class TestAmbryCUQuotaSource extends AmbryCUQuotaSource {
  public TestAmbryCUQuotaSource(QuotaConfig quotaConfig, AccountService accountService) throws IOException {
    super(quotaConfig, accountService);
  }

  public CapacityUnit getFeQuota() {
    return feQuota;
  }

  public CapacityUnit getFeUsage() {
    return feUsage.get();
  }

  public ConcurrentMap<String, CapacityUnit> getCuQuota() {
    return cuQuota;
  }

  public ConcurrentMap<String, CapacityUnit> getCuUsage() {
    return cuUsage;
  }
}
