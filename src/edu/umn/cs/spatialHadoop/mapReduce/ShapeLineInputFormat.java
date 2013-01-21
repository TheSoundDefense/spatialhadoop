package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


/**
 * An input format used with spatial data. It filters generated splits before
 * creating record readers.
 * @author eldawy
 *
 * @param <S>
 */
public class ShapeLineInputFormat extends SpatialInputFormat<LongWritable, Text> {
  
  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    this.rrClass = (Class<? extends RecordReader<LongWritable, Text>>) ShapeLineRecordReader.class;
    return super.getRecordReader(split, job, reporter);
  }
}
