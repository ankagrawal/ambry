/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.messageformat;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.DedupLogger;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A message write set that writes to the underlying write interface
 */
public class MessageFormatWriteSet implements MessageWriteSet {

  private static final Logger logger = LoggerFactory.getLogger(MessageFormatWriteSet.class);
  private final InputStream streamToWrite;
  private List<MessageInfo> streamInfo;
  private final DedupLogger dedupLogger;

  public MessageFormatWriteSet(InputStream streamToWrite, List<MessageInfo> streamInfo, boolean materializeStream)
      throws IOException {
    long sizeToWrite = 0;
    for (MessageInfo info : streamInfo) {
      sizeToWrite += info.getSize();
    }
    this.streamInfo = streamInfo;
    if (materializeStream) {
      this.streamToWrite = new ByteBufferInputStream(streamToWrite, (int) sizeToWrite);
    } else {
      this.streamToWrite = streamToWrite;
    }
    this.dedupLogger = null;
  }

  public MessageFormatWriteSet(InputStream streamToWrite, List<MessageInfo> streamInfo, boolean materializeStream, DedupLogger dedupLogger)
      throws IOException {
    long sizeToWrite = 0;
    for (MessageInfo info : streamInfo) {
      sizeToWrite += info.getSize();
    }
    this.streamInfo = streamInfo;
    if (materializeStream) {
      this.streamToWrite = new ByteBufferInputStream(streamToWrite, (int) sizeToWrite);
    } else {
      this.streamToWrite = streamToWrite;
    }
    this.dedupLogger = dedupLogger;
  }

  /**
   * @param writeChannel The write interface to write the messages to
   * @return
   * @throws StoreException
   */
  @Override
  public long writeTo(Write writeChannel) throws StoreException {
    return writeTo(writeChannel, null);
  }

  @Override
  public long writeTo(Write writeChannel, PartitionId partitionId) throws StoreException {
    ReadableByteChannel readableByteChannel = Channels.newChannel(streamToWrite);
    long sizeWritten = 0;
    for (MessageInfo info : streamInfo) {
      if (partitionId != null && dedupLogger != null) {
        try {
          ByteBuffer byteBuffer = ByteBuffer.allocate((int) info.getSize());
          readableByteChannel.read(byteBuffer);
          dedupLogger.log(info, byteBuffer, partitionId.toPathString());
          byteBuffer.flip();
          readableByteChannel = Channels.newChannel(new ByteArrayInputStream(byteBuffer.array()));
        } catch (IOException ioEx) {
          logger.error(String.format(info.getStoreKey().getID()));
          throw new StoreException(String.format(ioEx.toString()),
              StoreErrorCodes.IOError);
        } catch (NoSuchAlgorithmException nsEx) {
          logger.error(String.format(info.getStoreKey().getID()));
          throw new StoreException(String.format(nsEx.toString()),
              StoreErrorCodes.IOError);
        }
        logger.debug("Creating dedup log for {}".format(info.getStoreKey().getID()));
      }
      writeChannel.appendFrom(readableByteChannel, info.getSize());
      sizeWritten += info.getSize();
    }
    return sizeWritten;
  }

  @Override
  public List<MessageInfo> getMessageSetInfo() {
    return streamInfo;
  }
}
