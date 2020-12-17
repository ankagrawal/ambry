package com.github.ambry.quota.capacityunit;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.AmbryEnforcementRecommendation;
import com.github.ambry.quota.EnforcementRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.RequestQuotaEnforcer;
import com.github.ambry.rest.RestRequest;


public class AmbryCapacityUnitQuotaEnforcer implements RequestQuotaEnforcer {
  private final QuotaSource quotaSource;
  private final EnforcementRecommendation allowRecommendation;

  public AmbryCapacityUnitQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
    allowRecommendation =
        new AmbryEnforcementRecommendation(false, 0, AmbryCapacityUnitQuotaEnforcer.class.getSimpleName(), 200);
  }

  @Override
  public void init() {

  }

  @Override
  public EnforcementRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo) {
    return allowRecommendation;
  }

  @Override
  public EnforcementRecommendation recommend(RestRequest restRequest) {
    return allowRecommendation;
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {

  }
}
