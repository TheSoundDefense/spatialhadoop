package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.Progressable;


public class RTreeGridOutputFormat extends FileOutputFormat<CellInfo, TigerShape> {

  @Override
  public RecordWriter<CellInfo, TigerShape> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Output file name
    Path outFile = FileOutputFormat.getTaskOutputPath(job, name);

    // Get file system
    FileSystem fileSystem = outFile.getFileSystem(job);

    // Get grid info
    GridInfo gridInfo = new GridInfo();
    gridInfo.readFromString(job.get(GridOutputFormat.OUTPUT_GRID));
    CellInfo[] cellsInfo = GridOutputFormat.decodeCells(job.get(GridOutputFormat.OUTPUT_CELLS));
    return new RTreeGridRecordWriter(fileSystem, outFile, gridInfo, cellsInfo);
  }

}

