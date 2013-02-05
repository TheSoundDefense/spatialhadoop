package org.apache.hadoop.mapred.spatial;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
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
  
  public ShapeArrayRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    shapeClass = getShapeClass(conf);
  }
  
  public ShapeArrayRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    shapeClass = getShapeClass(job);
  }

  public ShapeArrayRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(CellInfo key, ArrayWritable shapes) throws IOException {
    // Get cellInfo for the current position in file
    boolean element_read = nextShapes(shapes);
    key.set(cellInfo); // Set the cellInfo for the last block read
    return element_read;
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
