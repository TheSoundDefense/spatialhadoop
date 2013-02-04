package org.apache.hadoop.mapred.spatial;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.SimpleSpatialIndex;

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
   * Selects the blocks that need to be processed b a MapReduce job.
   * @param gIndex
   * @param output
   */
  public void selectBlocks(SimpleSpatialIndex<BlockLocation> gIndex,
      ResultCollector<BlockLocation> output);
  
  /**
   * Selects block pairs that need to be processed together by a binary
   * MapReduce job. A binary MapReduce job is a job that deals with two input
   * files that need to be processed together (e.g., spatial join).
   * @param gIndex1
   * @param gIndex2
   */
  public void selectBlockPairs(SimpleSpatialIndex<BlockLocation> gIndex1,
      SimpleSpatialIndex<BlockLocation> gIndex2,
      ResultCollector2<BlockLocation, BlockLocation> output);
}
