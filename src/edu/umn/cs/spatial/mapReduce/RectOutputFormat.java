package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatial.TigerShape;

public class RectOutputFormat extends FileOutputFormat<LongWritable, TigerShape> {
  public static final String OUTPUT_GRID = "edu.umn.cs.spatial.mapReduce.RectOutputFormat.GridInfo";

  @Override
  public RecordWriter<LongWritable, TigerShape> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Output file name
    Path outFile = FileOutputFormat.getTaskOutputPath(job, name);

    // Get file system
    FileSystem fileSystem = outFile.getFileSystem(job);

    // Get grid info
    String gridStr = job.get(OUTPUT_GRID, "0,0,0,0,0,0");
    GridInfo gridInfo = new GridInfo();
    gridInfo.readFromString(gridStr);
    return new TigerShapeRecordWriter(fileSystem, outFile, gridInfo);
  }
}

