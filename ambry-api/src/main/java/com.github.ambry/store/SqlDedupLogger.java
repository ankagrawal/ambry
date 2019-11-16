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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;


public class SqlDedupLogger implements DedupLogger {
  private static final int BLOB_HEADER_SIZE = 270;

  String getMD5(byte[] data) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(data);
    return DatatypeConverter.printHexBinary(md.digest());
  }

  private static final SqlDedupLogger sqlDedupLogger = new SqlDedupLogger();

  public static SqlDedupLogger getSqlDedupLogger() {
    return sqlDedupLogger;
  }

  @Override
  public void log(MessageInfo messageInfo, ByteBuffer byteBuffer, String partitionId) throws NoSuchAlgorithmException {
    byte[] blob = byteBuffer.array();
    byte[] blobData = Arrays.copyOfRange(blob, 270, blob.length);
    String dataHash = getMD5(blobData);
    String blobHash = getMD5(blob);
    InMemoryDedupLog.getBlobInMemoryDedupLog().insert(messageInfo, blobHash, partitionId);
    InMemoryDedupLog.getDataInMemoryDedupLog().insert(messageInfo, dataHash, partitionId);
  }
}
