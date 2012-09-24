/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import java.io.*;

import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.io.*;
import org.apache.hadoop.spatial.CellInfo;

/**************************************************
 * A Block is a Hadoop FS primitive, identified by a 
 * long.
 *
 **************************************************/
public class Block implements Writable, Comparable<Block> {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (Block.class,
       new WritableFactory() {
         public Writable newInstance() { return new Block(); }
       });
  }

  // generation stamp of blocks that pre-date the introduction of
  // a generation stamp.
  public static final long GRANDFATHER_GENERATION_STAMP = 0;

  /**
   */
  public static boolean isBlockFilename(File f) {
    String name = f.getName();
    if ( name.startsWith( "blk_" ) && 
        name.indexOf( '.' ) < 0 ) {
      return true;
    } else {
      return false;
    }
  }

  static long filename2id(String name) {
    return Long.parseLong(name.substring("blk_".length()));
  }

  private long blockId;
  private long numBytes;
  private long generationStamp;
  /**
   * Info of the cell associated with this block.
   * Set to <code>null</code> when the block is for non-spatial file. 
   */
  private CellInfo cellInfo;

  public Block() {this(0, 0, 0, null);}

  public Block(final long blkid, final long len, final long generationStamp, CellInfo cellInfo) {
    set(blkid, len, generationStamp, cellInfo);
  }

  public Block(final long blkid, final long len, final long generationStamp) {
    set(blkid, len, generationStamp, null);
  }

  public Block(final long blkid) {this(blkid, 0, GenerationStamp.WILDCARD_STAMP, null);}

  public Block(Block blk) {this(blk.blockId, blk.numBytes, blk.generationStamp, blk.cellInfo);}

  /**
   * Find the blockid from the given filename
   */
  public Block(File f, long len, long genstamp) {
    this(filename2id(f.getName()), len, genstamp, null);
  }

  public void set(long blkid, long len, long genStamp, CellInfo cellInfo) {
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genStamp;
    this.cellInfo = cellInfo;
  }
  /**
   */
  public long getBlockId() {
    return blockId;
  }
  
  public void setBlockId(long bid) {
    blockId = bid;
  }

  /**
   */
  public String getBlockName() {
    return "blk_" + String.valueOf(blockId);
  }

  /**
   */
  public long getNumBytes() {
    return numBytes;
  }
  public void setNumBytes(long len) {
    this.numBytes = len;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }
  
  public void setGenerationStamp(long stamp) {
    generationStamp = stamp;
  }

  /**
   */
  public String toString() {
    return getBlockName() + "_" + getGenerationStamp();
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
    if (cellInfo == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      cellInfo.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
    if (in.readBoolean()) {
      if (this.cellInfo == null)
        this.cellInfo = new CellInfo(in);
      else
        this.cellInfo.readFields(in);
    } else {
      this.cellInfo = null;
    }
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes);
    }
  }

  /////////////////////////////////////
  // Comparable
  /////////////////////////////////////
  static void validateGenerationStamp(long generationstamp) {
    if (generationstamp == GenerationStamp.WILDCARD_STAMP) {
      throw new IllegalStateException("generationStamp (=" + generationstamp
          + ") == GenerationStamp.WILDCARD_STAMP");
    }    
  }

  /** {@inheritDoc} */
  public int compareTo(Block b) {
    //Wildcard generationStamp is NOT ALLOWED here
    validateGenerationStamp(this.generationStamp);
    validateGenerationStamp(b.generationStamp);

    if (blockId < b.blockId) {
      return -1;
    } else if (blockId == b.blockId) {
      return GenerationStamp.compare(generationStamp, b.generationStamp);
    } else {
      return 1;
    }
  }

  /** {@inheritDoc} */
  public boolean equals(Object o) {
    if (!(o instanceof Block)) {
      return false;
    }
    final Block that = (Block)o;
    //Wildcard generationStamp is ALLOWED here
    return this.blockId == that.blockId
      && GenerationStamp.equalsWithWildcard(
          this.generationStamp, that.generationStamp);
  }

  /** {@inheritDoc} */
  public int hashCode() {
    //GenerationStamp is IRRELEVANT and should not be used here
    return 37 * 17 + (int) (blockId^(blockId>>>32));
  }

  public CellInfo getCellInfo() {
    return cellInfo;
  }

  public void setCellInfo(CellInfo cellInfo) {
    this.cellInfo = cellInfo;
  }
}
