package edu.umn.cs.spatialHadoop.mapReduce;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatialHadoop.operations.SJMapReduce;

/**
 * Reads and parses a file that contains records of type Rectangle. Records are
 * assumed to be fixed size and of the format <id>,<left>,<top>,<right>,<bottom>
 * When a record of all zeros is encountered, it is assumed to be the end of
 * file. This means, no more records are processed after a zero-record. Records
 * are read one be one.
 * 
 * @author aseldawy
 * 
 */
public class SJInputFormat extends FileInputFormat<LongWritable, TigerShape> {

  @Override
  public RecordReader<LongWritable, TigerShape> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    // Set grid info in SJMapReduce
    SJMapReduce.cellsInfo = GridOutputFormat.decodeCells(job.get(SJMapReduce.CELLS_INFO));

    reporter.setStatus(split.toString());

    // Find index for the required file
    FileStatus[] files = listStatus(job);
    int i = 0;
    while (i < files.length
        && !files[i].getPath().equals(((FileSplit) split).getPath()))
      i++;
    return new TigerShapeRecordReader(job, (FileSplit) split, i);
  }
}
