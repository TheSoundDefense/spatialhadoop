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
  public static final String OUTPUT_CELLS = "edu.umn.cs.spatial.mapReduce.RectOutputFormat.CellsInfo";

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
    return new RTreeGridRecordWriter(fileSystem, outFile, gridInfo, extractCells(job.get(OUTPUT_CELLS)));
  }
  
  public static CellInfo[] extractCells(String encodedCells) {
    String[] parts = encodedCells.split(";");
    CellInfo[] cellsInfo = new CellInfo[parts.length];
    for (int i = 0; i < parts.length; i++) {
      cellsInfo[i] = new CellInfo();
      cellsInfo[i].readFromString(parts[i]);
    }
    return cellsInfo;
  }
}

