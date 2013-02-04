package org.apache.hadoop.mapred.spatial;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.ResultCollector2;
import org.apache.hadoop.spatial.SimpleSpatialIndex;
import org.apache.hadoop.spatial.SpatialSite;


class IndexedRectangle extends Rectangle {
  int index;

  public IndexedRectangle(int index, Rectangle r) {
    super(r);
    this.index = index;
  }
  
  @Override
  public boolean equals(Object obj) {
    return index == ((IndexedRectangle)obj).index;
  }
}

/**
 * An input format that reads a pair of files simultaneously and returns
 * a key for one of them and the value as a pair of values.
 * It generates a CombineFileSplit for each pair of blocks returned by the
 * BlockFilter. 
 * @author eldawy
 *
 */
public abstract class BinarySpatialInputFormat<K extends Writable, V extends Writable>
    extends FileInputFormat<PairWritable<K>, PairWritable<V>> {

  @SuppressWarnings("unchecked")
  @Override
  public InputSplit[] getSplits(final JobConf job, int numSplits) throws IOException {
    // Get a list of all input files. There should be exactly two files.
    final FileStatus[] inputFiles = listStatus(job);
    SimpleSpatialIndex<BlockLocation> gIndexes[] =
        new SimpleSpatialIndex[inputFiles.length];
    
    BlockFilter blockFilter = null;
    try {
      Class<? extends BlockFilter> blockFilterClass =
        job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass != null) {
        // Get all blocks the user wants to process
        blockFilter = blockFilterClass.newInstance();
        blockFilter.configure(job);
      }
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }

    if (blockFilter != null) {
      // Extract global indexes from input files

      for (int i_file = 0; i_file < inputFiles.length; i_file++) {
        FileSystem fs = inputFiles[i_file].getPath().getFileSystem(job);
        gIndexes[i_file] = fs.getGlobalIndex(inputFiles[i_file]);
      }
    }
    
    final Vector<CombineFileSplit> matchedSplits = new Vector<CombineFileSplit>();
    if (gIndexes[0] == null || gIndexes[1] == null) {
      // TODO join every possible pair (Cartesian product)
    } else {
      // Filter block pairs by the BlockFilter
      blockFilter.selectBlockPairs(gIndexes[0], gIndexes[1],
        new ResultCollector2<BlockLocation, BlockLocation>() {
          @Override
          public void collect(BlockLocation r, BlockLocation s) {
              try {
                FileSplit fsplit1 = new FileSplit(inputFiles[0].getPath(),
                    r.getOffset(), r.getLength(), r.getHosts());
                FileSplit fsplit2 = new FileSplit(inputFiles[1].getPath(),
                    s.getOffset(), s.getLength(), s.getHosts());
                matchedSplits.add((CombineFileSplit) FileSplitUtil
                    .combineFileSplits(job, fsplit1, fsplit2));
              } catch (IOException e) {
                e.printStackTrace();
              }
          }
        }
      );
    }

    LOG.info("Matched "+matchedSplits.size()+" combine splits");

    // Return all matched splits
    return matchedSplits.toArray(new InputSplit[matchedSplits.size()]);
  }

}
