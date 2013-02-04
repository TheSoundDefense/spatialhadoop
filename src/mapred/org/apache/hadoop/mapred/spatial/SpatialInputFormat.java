package org.apache.hadoop.mapred.spatial;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.ResultCollector;
import org.apache.hadoop.spatial.SimpleSpatialIndex;
import org.apache.hadoop.spatial.SpatialSite;

/**
 * An input format used with spatial data. It filters generated splits before
 * creating record readers.
 * @author eldawy
 *
 */
public abstract class SpatialInputFormat<K, V> extends FileInputFormat<K, V> {

  @SuppressWarnings("rawtypes")
  static final Class [] constructorSignature = new Class [] 
      { Configuration.class, 
       FileSplit.class};
  
  @SuppressWarnings("rawtypes")
  protected Class<? extends RecordReader> rrClass;
  
  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    if (split instanceof CombineFileSplit) {
      return new CombineFileRecordReader<K, V>(job, (CombineFileSplit)split,
          reporter, (Class<RecordReader<K, V>>) rrClass);
    } else if (split instanceof FileSplit) {
      try {
        @SuppressWarnings("rawtypes")
        Constructor<? extends RecordReader> rrConstructor;
        rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
        rrConstructor.setAccessible(true);
        return rrConstructor.newInstance(new Object [] {job, (FileSplit)split});
      } catch (SecurityException e) {
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
      }
      return null;
    } else {
      throw new RuntimeException("Cannot handle splits of type "+split.getClass());
    }
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {
      BlockFilter blockFilter = null;
      Class<? extends BlockFilter> blockFilterClass =
        job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass != null) {
        // Get all blocks the user wants to process
        blockFilter = blockFilterClass.newInstance();
        blockFilter.configure(job);
      }

      Vector<FileSplit> splits_2b_processed = new Vector<FileSplit>();
      for (Path file : getInputPaths(job)) {
        FileSystem fs = file.getFileSystem(job);
        long length = fs.getFileStatus(file).getLen();

        SimpleSpatialIndex<BlockLocation> gIndex = null;
        if (blockFilter != null)
          gIndex = fs.getGlobalIndex(fs.getFileStatus(file));

        final Collection<BlockLocation> blocks_2b_processed = new Vector<BlockLocation>();
        if (gIndex != null) {
          // Select blocks to be processed by applying the block filter
          blockFilter.selectBlocks(gIndex, new ResultCollector<BlockLocation>() {
              @Override
              public void collect(BlockLocation r) {
                blocks_2b_processed.add(r);
              }
            }
          );
        } else {
          // Either no global index (heap file) or no block filter configured
          // Process all blocks
          for (BlockLocation blk : fs.getFileBlockLocations(fs.getFileStatus(file), 0, length)) {
            blocks_2b_processed.add(blk);
          }
        }

        for (BlockLocation blockLocation : blocks_2b_processed) {
          splits_2b_processed.add(new FileSplit(file,
              blockLocation.getOffset(), blockLocation.getLength(),
              blockLocation.getHosts()));
        }
      }
      LOG.info("Number of splits to be processed "+splits_2b_processed.size());
      // If splits generated so far are less required by user, just return
      // them
      if (splits_2b_processed.size() <= numSplits ||
          numSplits == 0 ||
          !job.getBoolean(SpatialSite.AutoCombineSplits, true))
        return splits_2b_processed.toArray(
            new InputSplit[splits_2b_processed.size()]);

      // Combine these splits to form exactly n splits as advised by user
      return FileSplitUtil.autoCombineSplits(job, splits_2b_processed, numSplits);
    } catch (InstantiationException e) {
      return super.getSplits(job, numSplits);
    } catch (IllegalAccessException e) {
      return super.getSplits(job, numSplits);
    }
  }

  
}
