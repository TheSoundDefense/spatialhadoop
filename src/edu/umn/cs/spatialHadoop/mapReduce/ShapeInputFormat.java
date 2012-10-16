package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.Shape;


public class ShapeInputFormat extends FileInputFormat<LongWritable, Shape> {

  @Override
  public RecordReader<LongWritable, Shape> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    // Create record reader
    reporter.setStatus(split.toString());
    return new ShapeRecordReader(job, (FileSplit)split);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // Generate splits for all input paths
    InputSplit[] splits = super.getSplits(job, numSplits);
    return SplitCalculator.filterSplits(job, splits);
  }

}
