package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.edu.spatial.Rectangle;

/**
 * Combines two record readers to read pair of files in the same time.
 * It is used to generate joins. Each generated item is a rectangle from
 * first file and a rectangle from second file.
 * It is like a cartesian product.
 * @author aseldawy
 *
 */
public class RQCombineRecordReader implements RecordReader<Rectangle, Rectangle>{
  private RQRectangleRecordReader queryRectangles;
  private RQRectangleRecordReader inputRectangles;
  private boolean firstTime;
  private IntWritable dummyQueryRectangleId;
  private IntWritable dummyInputRectangleId;
  private final Configuration job;
  private final Reporter reporter;
  private final PairOfFileSplits twoSplits;
  private long totalSize;
  
  public RQCombineRecordReader(PairOfFileSplits twoSplits,
      Configuration job, Reporter reporter) throws IOException {
    this.twoSplits = twoSplits;
    this.job = job;
    this.reporter = reporter;
    queryRectangles = new RQRectangleRecordReader(twoSplits.fileSplit1, job, reporter);
    inputRectangles = new RQRectangleRecordReader(twoSplits.fileSplit2, job, reporter);
    firstTime = true;
    totalSize = twoSplits.fileSplit1.getLength() + twoSplits.fileSplit2.getLength();
  }

  public boolean next(Rectangle queryRectangle, Rectangle inputRectangle) throws IOException {
    // Read first query rectangle (key)
    if (firstTime) {
      if (!queryRectangles.next(dummyQueryRectangleId, queryRectangle)) {
        // All input has finished
        return false;
      }
      firstTime = false;
    }
    
    // Read next input rectangle
    while (!inputRectangles.next(dummyInputRectangleId, inputRectangle)) {
      // The input file has finished. Reopen from beginning and skip to next query rectangle.
      if (!queryRectangles.next(dummyQueryRectangleId, queryRectangle)) {
        // All input has finished
        return false;
      }
      // Close input file
      inputRectangles.close();
      // reopen from beginning
      inputRectangles = new RQRectangleRecordReader(twoSplits.fileSplit2, job, reporter);
    }
    
    return true;
  }

  public Rectangle createKey() {
    dummyQueryRectangleId = queryRectangles.createKey();
    return queryRectangles.createValue();
  }

  public Rectangle createValue() {
    dummyInputRectangleId = inputRectangles.createKey();
    return inputRectangles.createValue();
  }

  public long getPos() throws IOException {
    long pos = (long) (queryRectangles.getProgress() * totalSize +
      inputRectangles.getProgress() * twoSplits.fileSplit2.getLength());
    return pos;
  }

  public void close() throws IOException {
    inputRectangles.close();
    queryRectangles.close();
  }

  public float getProgress() throws IOException {
    return getPos() / totalSize;
  }
  
}
