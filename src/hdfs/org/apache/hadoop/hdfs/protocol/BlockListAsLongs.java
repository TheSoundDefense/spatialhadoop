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

import org.apache.hadoop.spatial.CellInfo;

/**
 * This class provides an interface for accessing list of blocks that
 * has been implemented as long[].
 * This class is usefull for block report. Rather than send block reports
 * as a Block[] we can send it as a long[].
 *
 */
public class BlockListAsLongs {
  /**
   * A block as 6 longs
   *   block-id and block length and generation stamp
   *   Followed by CellId and MBR of the block in case of spatial block.
   *   For a non-spatial block, it can be filled with -1's
   */
  private static final int LONGS_PER_BLOCK = 8;
  
  private static int index2BlockId(int index) {
    return index*LONGS_PER_BLOCK;
  }
  private static int index2BlockLen(int index) {
    return (index*LONGS_PER_BLOCK) + 1;
  }
  private static int index2BlockGenStamp(int index) {
    return (index*LONGS_PER_BLOCK) + 2;
  }
  private static int index2BlockCellId(int index) {
    return (index*LONGS_PER_BLOCK) + 3;
  }
  private static int index2BlockX(int index) {
    return (index*LONGS_PER_BLOCK) + 4;
  }
  private static int index2BlockY(int index) {
    return (index*LONGS_PER_BLOCK) + 5;
  }
  private static int index2BlockWidth(int index) {
    return (index*LONGS_PER_BLOCK) + 6;
  }
  private static int index2BlockHeight(int index) {
    return (index*LONGS_PER_BLOCK) + 7;
  }
  
  private long[] blockList;
  
  /**
   * Converting a block[] to a long[]
   * @param blockArray - the input array block[]
   * @return the output array of long[]
   */
  
  public static long[] convertToArrayLongs(final Block[] blockArray) {
    long[] blocksAsLongs = new long[blockArray.length * LONGS_PER_BLOCK];

    BlockListAsLongs bl = new BlockListAsLongs(blocksAsLongs);
    assert bl.getNumberOfBlocks() == blockArray.length;

    for (int i = 0; i < blockArray.length; i++) {
      bl.setBlock(i, blockArray[i]);
    }
    return blocksAsLongs;
  }

  /**
   * Constructor
   * @param iBlockList - BlockListALongs create from this long[] parameter
   */
  public BlockListAsLongs(final long[] iBlockList) {
    if (iBlockList == null) {
      blockList = new long[0];
    } else {
      if (iBlockList.length%LONGS_PER_BLOCK != 0) {
        // must be multiple of LONGS_PER_BLOCK
        throw new IllegalArgumentException();
      }
      blockList = iBlockList;
    }
  }

  
  /**
   * The number of blocks
   * @return - the number of blocks
   */
  public int getNumberOfBlocks() {
    return blockList.length/LONGS_PER_BLOCK;
  }
  
  
  /**
   * The block-id of the indexTh block
   * @param index - the block whose block-id is desired
   * @return the block-id
   */
  public long getBlockId(final int index)  {
    return blockList[index2BlockId(index)];
  }
  
  /**
   * The block-len of the indexTh block
   * @param index - the block whose block-len is desired
   * @return - the block-len
   */
  public long getBlockLen(final int index)  {
    return blockList[index2BlockLen(index)];
  }

  /**
   * The generation stamp of the indexTh block
   * @param index - the block whose block-len is desired
   * @return - the generation stamp
   */
  public long getBlockGenStamp(final int index)  {
    return blockList[index2BlockGenStamp(index)];
  }

  public CellInfo getCellInfo(final int index)  {
    if (blockList[index2BlockCellId(index)] == -1)
      return null;
    else
      return new CellInfo(
          blockList[index2BlockCellId(index)],
          blockList[index2BlockX(index)],
          blockList[index2BlockY(index)],
          blockList[index2BlockWidth(index)],
          blockList[index2BlockHeight(index)]
      );
  }
  
  /**
   * Set the indexTh block
   * @param index - the index of the block to set
   * @param b - the block is set to the value of the this block
   */
  void setBlock(final int index, final Block b) {
    blockList[index2BlockId(index)] = b.getBlockId();
    blockList[index2BlockLen(index)] = b.getNumBytes();
    blockList[index2BlockGenStamp(index)] = b.getGenerationStamp();
    if (b.getCellInfo() != null) {
      blockList[index2BlockCellId(index)] = b.getCellInfo().cellId;
      blockList[index2BlockX(index)] = b.getCellInfo().x;
      blockList[index2BlockY(index)] = b.getCellInfo().y;
      blockList[index2BlockWidth(index)] = b.getCellInfo().width;
      blockList[index2BlockHeight(index)] = b.getCellInfo().height;
    } else {
      blockList[index2BlockCellId(index)] = -1;
    }
  }
}
