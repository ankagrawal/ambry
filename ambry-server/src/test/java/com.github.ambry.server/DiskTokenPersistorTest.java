package com.github.ambry.server;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockDiskId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;


/**
 * Test for {@class DiskTokenPersistor}
 */
public class DiskTokenPersistorTest {
  /**
   * create protected final Map<String, List<PartitionInfo>> partitionGroupedByMountPath, clusterMap and findTokenHelper
   * create disk token persistor and call write with false and true
   *
   * create mergedclustermap and mergedpartition id as subclasses for MockClusterMap and MockParitionId
   * in mockclustermap add datanodes for vcr
   * in mockpartitionid add cloud replicas for partitions
   *
   * Now once we have the clustermap, then calculate protected final Map<String, List<PartitionInfo>> partitionGroupedByMountPath; (sample code in ReplicationManager)
   */

  /**
   * ClusterMap containing both store and vcr nodes
   */
  class MergedMockClusterMap extends MockClusterMap {
    private MockDataNodeId vcrNode;

    public MergedMockClusterMap() throws IOException {
      this(false, 1, 1, 1, false, 1);
    }

    private MergedMockClusterMap(boolean enableSSLPorts, int numNodes, int numMountPointsPerNode,
        int numDefaultStoresPerMountPoint, boolean createOnlyDefaultPartitionClass, int numVcrNodes) throws IOException {
      super(enableSSLPorts, numNodes, numMountPointsPerNode, numDefaultStoresPerMountPoint, createOnlyDefaultPartitionClass);
      String vcrDc = "vcrDC";
      MockDataNodeId localNode = getDataNodes().get(0);
      MockDataNodeId vcrNode = createVcrNode(getListOfPorts(PLAIN_TEXT_PORT_START_NUMBER+numNodes+1), vcrDc);
      dataNodes.add(vcrNode);
      dataCentersInClusterMap.add(vcrDc);
      dcToDataNodes.computeIfAbsent(vcrDc, name -> new ArrayList<>()).add(vcrNode);

      for(Long partitionId : partitions.keySet()) {
        MockPartitionId partition = (MockPartitionId) partitions.get(partitionId);
        MockVcrReplicaId vcrReplica = new MockVcrReplicaId(vcrNode.getPort(), partition, vcrNode, "/vcr/" + new Port(PLAIN_TEXT_PORT_START_NUMBER+numNodes+1, PortType.PLAINTEXT));
        partition.addReplica(vcrReplica);
        for(ReplicaId replicaId : partition.getReplicaIds()) {
          ((MockReplicaId) replicaId).addPeerReplica(vcrReplica);
        }
       }

      List<ReplicaId> localReplicaIds = getReplicaIds(localNode);
      for(ReplicaId replicaId : localReplicaIds) {
        PartitionId partitionId = replicaId.getPartitionId();
        Store = new MockStore().get
      }
    }
    /*
    we want to add one vcr node that has 3 cloud replicas
    such that now each replica will get this node's replica as a peer node


     */

    protected MockDataNodeId createVcrNode(ArrayList<Port> ports, String datacenter) throws IOException {
      List<String> mountPaths = new ArrayList<>(numMountPointsPerNode);
      File f = File.createTempFile("ambry", ".tmp");
      for(int i = 0; i < numMountPointsPerNode; i++) {
        mountPaths.add("/vcr/" + getPlainTextPort(ports) + i);
      }
      return new MockDataNodeId(ports, mountPaths, datacenter);
    }
  }

  class MockStore implements Store {
    @Override
    public void start() throws StoreException {

    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
      return null;
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {

    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {

    }

    @Override
    public void updateTtl(MessageWriteSet messageSetToUpdate) throws StoreException {

    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
      return null;
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
      return null;
    }

    @Override
    public StoreStats getStoreStats() {
      return null;
    }

    @Override
    public boolean isKeyDeleted(StoreKey key) throws StoreException {
      return false;
    }

    @Override
    public long getSizeInBytes() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean isStarted() {
      return false;
    }

    @Override
    public void shutdown() throws StoreException {

    }
  }

  /**
   * Partition having disk based as well as cloud vcr replicas
   */
  class MockVcrReplicaId extends MockReplicaId {
    public MockVcrReplicaId(int port, MockPartitionId partitionId, MockDataNodeId dataNodeId, String mountPath) {
      this.partitionId = partitionId;
      this.dataNodeId = dataNodeId;
      this.mountPath = mountPath;
      this.replicaPath = mountPath;
      this.diskId = new MockDiskId(dataNodeId, mountPath);
      isSealed = partitionId.getPartitionState().equals(PartitionState.READ_ONLY);
    }
  }


}
