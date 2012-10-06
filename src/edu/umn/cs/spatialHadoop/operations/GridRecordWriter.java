package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;

public class GridRecordWriter extends org.apache.hadoop.spatial.GridRecordWriter implements RecordWriter<CellInfo, Text> {

  public GridRecordWriter(FileSystem fileSystem, Path outFile, CellInfo[] cells, boolean overwrite) throws IOException {
    super(fileSystem, outFile, cells,  overwrite);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    super.close();
  }
}
