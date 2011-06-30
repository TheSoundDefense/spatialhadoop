package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
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
public class SJROCombineRecordReader implements RecordReader<CollectionWritable<Rectangle>, CollectionWritable<Rectangle>>{
  private LongWritable dummy1;
  private LongWritable dummy2;
  private RQRectangleRecordReader input1;
  private RQRectangleRecordReader input2;
  /**A temporary variable to read from file 1*/
  private Rectangle r;
  /**A temporary variable to read from file 2*/
  private Rectangle s;
  private final Configuration job;
  private final Reporter reporter;
  private final PairOfFileSplits twoSplits;
  private long totalSize;
  
  public SJROCombineRecordReader(PairOfFileSplits twoSplits,
      Configuration job, Reporter reporter) throws IOException {
    this.twoSplits = twoSplits;
    this.job = job;
    this.reporter = reporter;
    input1 = new RQRectangleRecordReader(job, twoSplits.fileSplit1);
    input2 = new RQRectangleRecordReader(job, twoSplits.fileSplit2);
    totalSize = twoSplits.fileSplit1.getLength() + twoSplits.fileSplit2.getLength();
  }

  public boolean next(CollectionWritable<Rectangle> R, CollectionWritable<Rectangle> S) throws IOException {
    // Read all rectangles from both files
    R.clear();
    S.clear();
    
    while (input1.next(dummy1, r)) {
      R.add((Rectangle) r.clone());
    }
    while (input2.next(dummy2, s)) {
      S.add((Rectangle) s.clone());
    }

    return !R.isEmpty() && !S.isEmpty();
  }

  public CollectionWritable<Rectangle> createKey() {
    dummy1 = input1.createKey();
    r = input1.createValue();
    return new ArrayListWritable<Rectangle>();
  }

  public CollectionWritable<Rectangle> createValue() {
    dummy2 = input2.createKey();
    s = input2.createValue();
    return new ArrayListWritable<Rectangle>();
  }

  public long getPos() throws IOException {
    long pos = (long) (input1.getProgress() + input2.getProgress());
    return pos;
  }

  public void close() throws IOException {
    input1.close();
    input1.close();
  }

  public float getProgress() throws IOException {
    return getPos() / totalSize;
  }
  
}
