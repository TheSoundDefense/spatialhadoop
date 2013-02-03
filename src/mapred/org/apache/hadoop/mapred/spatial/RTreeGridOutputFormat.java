package org.apache.hadoop.mapred.spatial;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.Progressable;

public class RTreeGridOutputFormat<S extends Shape> extends FileOutputFormat<IntWritable, Text> {

  @Override
  public RecordWriter<IntWritable, Text> getRecordWriter(FileSystem ignored,
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
    RTreeGridRecordWriter writer = new RTreeGridRecordWriter
        (fileSystem, outFile, cellsInfo, overwrite);
    writer.setStockObject(SpatialSite.createStockShape(job));
    return writer;
  }

}

