package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.mapReduce.GridOutputFormat;


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
    CellInfo[] cellsInfo = GridOutputFormat.decodeCells(job.get(GridOutputFormat.OUTPUT_CELLS));
    return new RTreeGridRecordWriter(fileSystem, outFile, cellsInfo);
  }

}

