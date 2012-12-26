package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;

public class GridRecordWriter
extends org.apache.hadoop.spatial.GridRecordWriter<Shape> implements RecordWriter<IntWritable, Text> {

  public GridRecordWriter(FileSystem fileSystem, Path outFile, CellInfo[] cells, boolean overwrite) throws IOException {
    super(fileSystem, outFile, cells,  overwrite);
  }
  
  @Override
  public void write(IntWritable key, Text value) throws IOException {
    super.write(key.get(), value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close(reporter);
  }
}
