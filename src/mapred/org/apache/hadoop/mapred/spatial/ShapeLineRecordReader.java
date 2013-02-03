package org.apache.hadoop.mapred.spatial;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.Shape;

/**
 * A record reader for objects of class {@link Shape}
 * @author eldawy
 *
 */
public class ShapeLineRecordReader
    extends SpatialRecordReader<LongWritable, Text> {

  public ShapeLineRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
  }

  public ShapeLineRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
  }
  
  public ShapeLineRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    super(in, offset, endOffset);
  }

  @Override
  public boolean next(LongWritable key, Text shapeLine) throws IOException {
    key.set(getPos());
    return nextLine(shapeLine);
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Text createValue() {
    return new Text();
  }
}
