package org.apache.hadoop.mapred.spatial;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.SimpleSpatialIndex;

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
  public void selectBlocks(SimpleSpatialIndex<BlockLocation> gIndex,
      ResultCollector<BlockLocation> output) {
    // Do nothing
  }

  @Override
  public void selectBlockPairs(SimpleSpatialIndex<BlockLocation> gIndex1,
      SimpleSpatialIndex<BlockLocation> gIndex2,
      ResultCollector2<BlockLocation, BlockLocation> output) {
    // Do nothing
  }

}
