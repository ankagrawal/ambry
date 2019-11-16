/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class InMemoryDedupLog {

  /**
   * Class containing information associated with blob metadata.
   */
  private class BlobInfo {
    private final String key;
    private final long size;
    private final long expirationTimeInMs;
    private final boolean isDeleted;
    private final boolean isTtlUpdated;
    private final short accountId;
    private final short containerId;
    private final String hash;
    private final String partitionId;

    //create table blobtable (blobid TEXT PRIMARY KEY, size INTEGER, expirationTimeInMs INTEGER, idDeleted INTEGER,
    // isTtlUpdated INTEGER, accountId INTEGER, containerId INTEGER, hash TEXT, partitionId TEXT);
    public BlobInfo(MessageInfo messageInfo, String hash, String partitionId) {
      this.key = messageInfo.getStoreKey().getID();
      this.size = messageInfo.getSize();
      this.expirationTimeInMs = messageInfo.getExpirationTimeInMs();
      this.isDeleted = messageInfo.isDeleted();
      this.isTtlUpdated = messageInfo.isTtlUpdated();
      this.accountId = messageInfo.getAccountId();
      this.containerId = messageInfo.getContainerId();
      this.hash = hash;
      this.partitionId = partitionId;
    }

    public String getKey() {
      return key;
    }

    public long getSize() {
      return size;
    }

    public long getExpirationTimeInMs() {
      return expirationTimeInMs;
    }

    public boolean isDeleted() {
      return isDeleted;
    }

    public boolean isTtlUpdated() {
      return isTtlUpdated;
    }

    public short getAccountId() {
      return accountId;
    }

    public short getContainerId() {
      return containerId;
    }

    public String getHash() {
      return hash;
    }
  }

  /**
   * Class containing hash information of unique hashes.
   */
  private class BlobHashInfo {
    final String hash;
    final long size;
    int count;
    final String partitionId;

    //create table hashtable (hash TEXT PRIMARY KEY, size INTEGER, count INTEGER, partitionId TEXT);
    public BlobHashInfo(String hash, long size, int count, String partitionId) {
      this.hash = hash;
      this.size = size;
      this.count = count;
      this.partitionId = partitionId;
    }

    public void incrCount() {
      count++;
    }

    public String getHash() {
      return hash;
    }

    public long getSize() {
      return size;
    }

    public int getCount() {
      return count;
    }
  }

  private final Map<String, BlobInfo> blobMap;
  private final Map<String, BlobHashInfo> blobHashMap;
  private final Connection connection = null;
  private static String blobInsert = "insert into blobtable (blobid, size, expirationTimeInMs, isDeleted, isTtlUpdated, accountId, containerId, hash, partitionId) values ({}, {}, {}, {}, {}, {}, {}, {}, {})";
  private static String hashInsert = "insert into hashtable (hash, size, count, partition) values ({}, {}, {}, {})";
  private static String getCountSelect = "select count from hashtable where hash = \"{}\"";
  private static String hashUpdate = "update hashtable set count = {} where hash = \"{}\"";


  public InMemoryDedupLog() {
    blobMap = new HashMap<>();
    blobHashMap = new HashMap<>();
    try {
      connection = DriverManager.getConnection("jdbc:sqlite:/home/ankuagra/ambrysqlite/replication.db");
    } catch (SQLException sqlEx) {
      System.out.println(sqlEx.toString());
    }
  }

  public static final InMemoryDedupLog inMemoryDedupLogForData = new InMemoryDedupLog();
  public static final InMemoryDedupLog inMemoryDedupLogForBlob = new InMemoryDedupLog();

  public static InMemoryDedupLog getDataInMemoryDedupLog() {
    return inMemoryDedupLogForData;
  }

  public static InMemoryDedupLog getBlobInMemoryDedupLog() {
    return inMemoryDedupLogForBlob;
  }

  //insert into hashtable (hash, size, count, paritionId) values ("abcd", 190, 10, "1");
  public synchronized void insert(MessageInfo messageInfo, String hash, String partitionId) throws SQLException {
    String blobid = messageInfo.getStoreKey().getID();
    BlobInfo blobInfo = new BlobInfo(messageInfo, hash, partitionId);
    blobMap.put(blobid, blobInfo);
    if (blobHashMap.containsKey(hash)) {
      blobHashMap.get(hash).incrCount();
    } else {
      blobHashMap.put(hash, new BlobHashInfo(hash, messageInfo.getSize(), 1, partitionId));
    }

    Statement statement = connection.createStatement();

    String blobInsertStmt = blobInsert.format(blobid, messageInfo.getSize(), messageInfo.getExpirationTimeInMs(), messageInfo.isDeleted(), messageInfo.isTtlUpdated(), messageInfo.getAccountId(), messageInfo.getContainerId(), hash, partitionId);
    String hashInsertStmt = hashInsert.format(hash, messageInfo.getSize(), 1, partitionId);

    statement.execute(blobInsertStmt);
    int count = getHashCount(hash, statement);
    if(count == 0) {
      statement.execute(hashInsertStmt);
    } else {
      String hashUpdateStmt = hashUpdate.format("" + count, hash);
      statement.execute(hashUpdateStmt);
    }
    connection.commit();
  }

  private int getHashCount(String hash, Statement statement) throws SQLException {
    String getCountSelectStmt = getCountSelect.format(hash);
    ResultSet rs = statement.executeQuery(getCountSelectStmt);
    int count = 0;
    if(rs.next()) {
      count = rs.getInt("count");
    }
    return count;
  }
}
