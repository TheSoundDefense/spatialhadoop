package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.util.Progressable;



public class RTreeGridOutputFormat extends FileOutputFormat<CellInfo, Shape> {

  @Override
  public RecordWriter<CellInfo, Shape> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Output file name
    Path outFile = FileOutputFormat.getTaskOutputPath(job, name);

    // Get file system
    FileSystem fileSystem = outFile.getFileSystem(job);
    
    boolean overwrite = job.getBoolean(GridOutputFormat.OVERWRITE, false);

    // Get grid info
    CellInfo[] cellsInfo = GridOutputFormat.decodeCells(job.get(GridOutputFormat.OUTPUT_CELLS));
    return new RTreeGridRecordWriter(fileSystem, outFile, cellsInfo, overwrite);
  }

}

