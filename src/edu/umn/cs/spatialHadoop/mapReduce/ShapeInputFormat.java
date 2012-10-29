package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.Shape;

import edu.umn.cs.spatialHadoop.SpatialSite;


public class ShapeInputFormat<S extends Shape> extends FileInputFormat<LongWritable, S> {

  @Override
  public RecordReader<LongWritable, S> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    // Create record reader
    reporter.setStatus(split.toString());
    return new ShapeRecordReader<S>(job, (FileSplit)split);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] inputSplits = super.getSplits(job, numSplits);
    try {
      Class<? extends BlockFilter> blockFilterClass =
          job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      
      if (blockFilterClass == null) {
        return inputSplits;
      }
    
      Vector<InputSplit> splits_2b_processed = new Vector<InputSplit>();

      // Get all blocks the user wants to process
      BlockFilter blockFilter = blockFilterClass.newInstance();
      blockFilter.configure(job);
      for (Path file : getInputPaths(job)) {
        FileSystem fs = file.getFileSystem(job);
        long lenght = fs.getFileStatus(file).getLen();
        BlockLocation[] blks =
            fs.getFileBlockLocations(fs.getFileStatus(file), 0l, lenght);
        
        Collection<BlockLocation> blocks_2b_processed = blockFilter.processBlocks(blks);
        // Return only splits in the ranges
        for (InputSplit split : inputSplits) {
          FileSplit fsplit = (FileSplit) split;
          if (fsplit.getPath().equals(file)) {
            Iterator<BlockLocation> blk_iter = blocks_2b_processed.iterator();
            while (blk_iter.hasNext()) {
              BlockLocation blk = blk_iter.next();
              if (fsplit.getStart() + fsplit.getLength() <= blk.getOffset() ||
                  blk.getOffset() + blk.getLength() <= fsplit.getStart()) {
              } else {
                splits_2b_processed.add(split);
                break;
              }
            }
          }
        }
      }
      
      return splits_2b_processed.toArray(
          new FileSplit[splits_2b_processed.size()]);
    } catch (InstantiationException e) {
      return inputSplits;
    } catch (IllegalAccessException e) {
      return inputSplits;
    }
  }
  
}
