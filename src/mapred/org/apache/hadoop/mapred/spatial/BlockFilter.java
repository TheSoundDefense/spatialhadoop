package org.apache.hadoop.mapred.spatial;

import java.util.Collection;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapred.JobConf;

/**
 * An interface for filtering blocks before running map tasks.
 * @author eldawy
 *
 */
public interface BlockFilter {

  /**
   * Configure the block filter the first time it is created.
   * @param conf
   */
  public void configure(JobConf job);

  /**
   * Returns true if the given block should be processed.
   * @param blk
   * @return
   */
  public boolean processBlock(BlockLocation blk);
  
  /**
   * Given a set of blocks, return only those blocks which should be processed.
   * @param blks
   * @return
   */
  public <T extends BlockLocation> Collection<T> processBlocks(T[] blks);
  
  /**
   * Returns true if the given pair of blocks should be combined together.
   * @param pair
   * @return
   */
  public boolean processPair(BlockLocation blk1, BlockLocation blk2);
  
  /**
   * Given two lists of blocks for two files, return all pairs that should
   * be combined together and processed by one map function.
   * @param pairs
   * @return
   */
  public <T extends BlockLocation> Collection<? extends PairWritable<T>>
      processPairs(T[] blks1, T[] blks2);
}
