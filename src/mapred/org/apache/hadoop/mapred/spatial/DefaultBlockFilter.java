package org.apache.hadoop.mapred.spatial;

import java.util.Collection;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapred.JobConf;

/**
 * A default implementation for BlockFilter that returns everything.
 * @author eldawy
 *
 */
public class DefaultBlockFilter implements BlockFilter {
  
  @Override
  public void configure(JobConf job) {
    // Do nothing
  }

  @Override
  public boolean processBlock(BlockLocation blk) {
    return true;
  }

  @Override
  public <T extends BlockLocation> Collection<T> processBlocks(T[] blks) {
    Vector<T> blocks_2b_processed = new Vector<T>();
    
    for (T blk : blks) {
      if (processBlock(blk))
        blocks_2b_processed.add(blk);
    }
    return blocks_2b_processed;
  }
  
  @Override
  public boolean processPair(BlockLocation blk1, BlockLocation blk2) {
    return true;
  }

  @Override
  public <T extends BlockLocation> Collection<? extends PairWritable<T>> processPairs(
      T[] blks1, T[] blks2) {
    Vector<PairWritable<T>> pairs_2b_processed = new Vector<PairWritable<T>>();
    for (T blk1 : blks1) {
      for (T blk2 : blks2) {
        if (processPair(blk1, blk2)) {
          pairs_2b_processed.add(new PairWritable<T>(blk1, blk2));
        }
      }
      
    }
    return pairs_2b_processed;
  }

}
