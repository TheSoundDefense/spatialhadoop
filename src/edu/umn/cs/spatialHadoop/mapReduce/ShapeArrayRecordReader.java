package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;


/**
 * Reads a file as a list of RTrees
 * @author eldawy
 *
 */
public class ShapeArrayRecordReader extends SpatialRecordReader<CellInfo, ArrayWritable> {
  public static final Log LOG = LogFactory.getLog(ShapeArrayRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private Class<? extends Shape> shapeClass;
  
  /**File system from where the file is read*/
  private FileSystem fs;
  
  /**Path of the file read*/
  private Path path;
  
  public ShapeArrayRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    path = split.getPath();
    fs = path.getFileSystem(job);
    shapeClass = getShapeClass(job);
  }

  public ShapeArrayRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(CellInfo key, ArrayWritable shapes) throws IOException {
    // Get cellInfo for the current position in file
    BlockLocation[] fileBlockLocations =
        fs.getFileBlockLocations(fs.getFileStatus(path), getPos(), 1);
    if (fileBlockLocations.length == 0)
      return false;
    key.set(fileBlockLocations[0].getCellInfo());
    return nextShapes(shapes);
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public ArrayWritable createValue() {
    return new ArrayWritable(shapeClass);
  }
  
  private Class<? extends Shape> getShapeClass(Configuration job) {
    String shapeClassName =
        job.get(SpatialSite.SHAPE_CLASS, Point.class.getName());
    try {
      Class<? extends Shape> shapeClass =
          Class.forName(shapeClassName).asSubclass(Shape.class);
      return shapeClass;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }
}
