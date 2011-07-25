package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.ArrayListWritable;
import edu.umn.cs.CollectionWritable;

/**
 * Combines two record readers to read pair of files in the same time.
 * It is used to generate joins. Each generated item is a rectangle from
 * first file and a rectangle from second file.
 * It is like a cartesian product.
 * @author aseldawy
 *
 */
public class CollectionPairRecordReader implements RecordReader<CollectionWritable<TigerShape>, CollectionWritable<TigerShape>>{
  private LongWritable dummy1;
  private LongWritable dummy2;
  private TigerShapeRecordReader input1;
  private TigerShapeRecordReader input2;
  /**A temporary variable to read from file 1*/
  private TigerShape r;
  /**A temporary variable to read from file 2*/
  private TigerShape s;
  private final Configuration job;
  private final Reporter reporter;
  private final PairOfFileSplits twoSplits;
  private long totalSize;
  
  public CollectionPairRecordReader(PairOfFileSplits twoSplits,
      Configuration job, Reporter reporter) throws IOException {
    this.twoSplits = twoSplits;
    this.job = job;
    this.reporter = reporter;
    input1 = new TigerShapeRecordReader(job, twoSplits.fileSplit1);
    input2 = new TigerShapeRecordReader(job, twoSplits.fileSplit2);
    totalSize = twoSplits.fileSplit1.getLength() + twoSplits.fileSplit2.getLength();
  }

  public boolean next(CollectionWritable<TigerShape> R, CollectionWritable<TigerShape> S) throws IOException {
    // Read all rectangles from both files
    R.clear();
    S.clear();
    
    while (input1.next(dummy1, r)) {
      R.add((TigerShape) r.clone());
    }
    while (input2.next(dummy2, s)) {
      S.add((TigerShape) s.clone());
    }

    return !R.isEmpty() && !S.isEmpty();
  }

  public CollectionWritable<TigerShape> createKey() {
    dummy1 = input1.createKey();
    r = input1.createValue();
    return new ArrayListWritable<TigerShape>();
  }

  public CollectionWritable<TigerShape> createValue() {
    dummy2 = input2.createKey();
    s = input2.createValue();
    return new ArrayListWritable<TigerShape>();
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
