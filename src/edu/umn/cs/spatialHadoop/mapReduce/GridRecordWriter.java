package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;

public class GridRecordWriter extends org.apache.hadoop.spatial.GridRecordWriter implements RecordWriter<CellInfo, Text> {

  public GridRecordWriter(FileSystem fileSystem, Path outFile, GridInfo gridInfo, CellInfo[] cells, boolean overwrite) throws IOException {
    super(fileSystem, outFile, gridInfo, cells,  overwrite);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close();
  }
}
