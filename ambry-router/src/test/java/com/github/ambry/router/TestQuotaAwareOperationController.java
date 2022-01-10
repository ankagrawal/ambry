package com.github.ambry.router;

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.List;
import java.util.Set;


public class TestQuotaAwareOperationController extends QuotaAwareOperationController {
  int pollCallCount = 0;
  public TestQuotaAwareOperationController(String suffix, String defaultPartitionClass, AccountService accountService,
      NetworkClientFactory networkClientFactory, ClusterMap clusterMap, RouterConfig routerConfig,
      ResponseHandler responseHandler, NotificationSystem notificationSystem, NonBlockingRouterMetrics routerMetrics,
      KeyManagementService kms, CryptoService cryptoService, CryptoJobHandler cryptoJobHandler, Time time,
      NonBlockingRouter nonBlockingRouter) throws IOException {
    super(suffix, defaultPartitionClass, accountService, networkClientFactory, clusterMap, routerConfig,
        responseHandler, notificationSystem, routerMetrics, kms, cryptoService, cryptoJobHandler, time,
        nonBlockingRouter);
  }

  @Override
  protected void pollForRequests(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
    super.pollForRequests(requestsToSend, requestsToDrop);
    pollCallCount++;
  }
}
