package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.edu.spatial.Point;
import edu.umn.edu.spatial.PointWithK;
import edu.umn.edu.spatial.Rectangle;

/**
 * Combines two record readers to read pair of files in the same time.
 * It is used to generate joins. Each generated item is a rectangle from
 * first file and a rectangle from second file.
 * It is like a cartesian product.
 * @author aseldawy
 *
 */
public class KNNCombineRecordReader implements RecordReader<PointWithK, Point>{
  private KNNPointRecordReader queryPoints;
  private KNNPointRecordReader inputPoints;
  private boolean firstTime;
  private IntWritable dummyQueryPointId;
  private IntWritable dummyInputPointId;
  private final Configuration job;
  private final Reporter reporter;
  private final PairOfFileSplits twoSplits;
  private long totalSize;
  private Point tempPoint;
  
  public KNNCombineRecordReader(PairOfFileSplits twoSplits,
      Configuration job, Reporter reporter) throws IOException {
    this.twoSplits = twoSplits;
    this.job = job;
    this.reporter = reporter;
    queryPoints = new KNNPointRecordReader(job, twoSplits.fileSplit1);
    inputPoints = new KNNPointRecordReader(job, twoSplits.fileSplit2);
    firstTime = true;
    totalSize = twoSplits.fileSplit1.getLength() + twoSplits.fileSplit2.getLength();
  }

  public boolean next(PointWithK queryPoint, Point inputPoint) throws IOException {
    // Read first query rectangle (key)
    if (firstTime) {
      if (!queryPoints.next(dummyQueryPointId, tempPoint)) {
        // All input has finished
        return false;
      }
      // Convert read query point to PointWithK
      queryPoint.x = tempPoint.x;
      queryPoint.y = tempPoint.y;
      queryPoint.k = tempPoint.type;
      firstTime = false;
    }
    
    // Read next input rectangle
    while (!inputPoints.next(dummyInputPointId, inputPoint)) {
      // The input file has finished. Reopen from beginning and skip to next query rectangle.
      if (!queryPoints.next(dummyQueryPointId, tempPoint)) {
        // All input has finished
        return false;
      }
      // Convert read query point to PointWithK
      queryPoint.x = tempPoint.x;
      queryPoint.y = tempPoint.y;
      queryPoint.k = tempPoint.type;
      // Close input file
      inputPoints.close();
      // reopen from beginning
      inputPoints = new KNNPointRecordReader(job, twoSplits.fileSplit2);
    }
    
    return true;
  }

  public PointWithK createKey() {
    dummyQueryPointId = queryPoints.createKey();
    this.tempPoint = queryPoints.createValue();
    return new PointWithK();
  }

  public Point createValue() {
    dummyInputPointId = inputPoints.createKey();
    return inputPoints.createValue();
  }

  public long getPos() throws IOException {
    long pos = (long) (queryPoints.getProgress() * totalSize +
      inputPoints.getProgress() * twoSplits.fileSplit2.getLength());
    return pos;
  }

  public void close() throws IOException {
    inputPoints.close();
    queryPoints.close();
  }

  public float getProgress() throws IOException {
    return getPos() / totalSize;
  }
  
}
