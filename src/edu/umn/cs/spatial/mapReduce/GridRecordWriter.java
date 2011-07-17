package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.TigerShape;

public class GridRecordWriter extends org.apache.hadoop.spatial.GridRecordWriter implements RecordWriter<CellInfo, TigerShape> {

  public GridRecordWriter(FileSystem fileSystem, Path outFile, GridInfo gridInfo) {
    super(fileSystem, outFile, gridInfo);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close();
  }
}
