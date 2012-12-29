package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Point;
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
    writer.setStockObject(createStockShape(job));
    return writer;
  }

  private S createStockShape(Configuration job) {
    S stockShape = null;
    String shapeClassName = job.get(SpatialSite.SHAPE_CLASS, Point.class.getName());
    try {
      Class<? extends Shape> shapeClass =
          Class.forName(shapeClassName).asSubclass(Shape.class);
      stockShape = (S) shapeClass.newInstance();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return stockShape;
  }

}

