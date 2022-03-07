package com.github.ambry.router;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaEnforcerFactory;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcer;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory;
import com.github.ambry.rest.RestRequest;


public class RejectingQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final RejectingQuotaEnforcer rejectingQuotaEnforcer;

  /**
   * Constructor for {@link AmbryCUQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   */
  public RejectingQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore) {
    rejectingQuotaEnforcer = new RejectingQuotaEnforcer(quotaSource, quotaConfig);
  }

  @Override
  public QuotaEnforcer getQuotaEnforcer() {
    return rejectingQuotaEnforcer;
  }
}

class RejectingQuotaEnforcer extends AmbryCUQuotaEnforcer {
  public RejectingQuotaEnforcer(QuotaSource quotaSource, QuotaConfig quotaConfig) {
    super(quotaSource, quotaConfig);
  }

  /**
   * Build the {@link QuotaRecommendation} object from the specified usage and {@link QuotaName}.
   * @param usage percentage usage.
   * @param quotaName {@link QuotaName} object.
   * @return QuotaRecommendation object.
   */
  protected QuotaRecommendation buildQuotaRecommendation(float usage, QuotaName quotaName) {
    QuotaAction quotaAction = (usage >= MAX_USAGE_PERCENTAGE_ALLOWED) ? QuotaAction.REJECT : QuotaAction.ALLOW;
    return new QuotaRecommendation(quotaAction, usage, quotaName,
        (quotaAction == QuotaAction.REJECT) ? throttleRetryAfterMs : QuotaRecommendation.NO_THROTTLE_RETRY_AFTER_MS);
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException {
    return false;
  }
}