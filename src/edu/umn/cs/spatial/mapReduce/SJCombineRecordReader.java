package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.spatial.Rectangle;

/**
 * Combines two record readers to read pair of files in the same time.
 * It is used to generate joins. Each generated item is a rectangle from
 * first file and a rectangle from second file.
 * It is like a cartesian product.
 * @author aseldawy
 *
 */
public class SJCombineRecordReader implements RecordReader<GridInfo, Rectangle>{
  private SJGridInfoRecordReader queryGridInfo;
  private RQRectangleRecordReader inputRectangles;
  private boolean firstTime;
  private LongWritable dummyGridInfoId;
  private LongWritable dummyInputRectangleId;
  private final Configuration job;
  private final Reporter reporter;
  private final PairOfFileSplits twoSplits;
  private long totalSize;
  
  public SJCombineRecordReader(PairOfFileSplits twoSplits,
      Configuration job, Reporter reporter) throws IOException {
    this.twoSplits = twoSplits;
    this.job = job;
    this.reporter = reporter;
    queryGridInfo = new SJGridInfoRecordReader(job, twoSplits.fileSplit1);
    inputRectangles = new RQRectangleRecordReader(job, twoSplits.fileSplit2);
    firstTime = true;
    totalSize = twoSplits.fileSplit1.getLength() + twoSplits.fileSplit2.getLength();
  }

  public boolean next(GridInfo gridInfo, Rectangle inputRectangle) throws IOException {
    // Read first query rectangle (key)
    if (firstTime) {
      if (!queryGridInfo.next(dummyGridInfoId, gridInfo)) {
        // All input has finished
        return false;
      }
      firstTime = false;
    }
    
    // Read next input rectangle
    while (!inputRectangles.next(dummyInputRectangleId, inputRectangle)) {
      // The input file has finished. Reopen from beginning and skip to next query rectangle.
      if (!queryGridInfo.next(dummyGridInfoId, gridInfo)) {
        // All input has finished
        return false;
      }
      // Close input file
      inputRectangles.close();
      // reopen from beginning
      inputRectangles = new RQRectangleRecordReader(job, twoSplits.fileSplit2);
    }
    
    return true;
  }

  public GridInfo createKey() {
    dummyGridInfoId = queryGridInfo.createKey();
    return queryGridInfo.createValue();
  }

  public Rectangle createValue() {
    dummyInputRectangleId = inputRectangles.createKey();
    return inputRectangles.createValue();
  }

  public long getPos() throws IOException {
    long pos = (long) (queryGridInfo.getProgress() * totalSize +
      inputRectangles.getProgress() * twoSplits.fileSplit2.getLength());
    return pos;
  }

  public void close() throws IOException {
    inputRectangles.close();
    queryGridInfo.close();
  }

  public float getProgress() throws IOException {
    return getPos() / totalSize;
  }
  
}
