package org.apache.hadoop.mapred.spatial;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;


/**
 * Reads a file as a list of RTrees
 * @author eldawy
 *
 */
public class RTreeRecordReader<S extends Shape> extends SpatialRecordReader<CellInfo, RTree<S>> {
  public static final Log LOG = LogFactory.getLog(RTreeRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private S stockShape;
  
  /**File system from where the file is read*/
  private FileSystem fs;
  
  /**Path of the file read*/
  private Path path;
  
  public RTreeRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    path = split.getPath(index);
    fs = path.getFileSystem(conf);
    stockShape = createStockShape(conf);
  }
  
  public RTreeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    path = split.getPath();
    fs = path.getFileSystem(job);
    stockShape = createStockShape(job);
  }

  public RTreeRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(CellInfo key, RTree<S> rtree) throws IOException {
    // Get cellInfo for the current position in file
    BlockLocation[] fileBlockLocations =
        fs.getFileBlockLocations(fs.getFileStatus(path), getPos(), 1);
    if (fileBlockLocations.length == 0)
      return false;
    key.set(fileBlockLocations[0].getCellInfo());
    return nextRTree(rtree);
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public RTree<S> createValue() {
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject(stockShape);
    return rtree;
  }
  
  @SuppressWarnings("unchecked")
  private S createStockShape(Configuration job) {
    S stockShape = null;
    String shapeClassName =
        job.get(SpatialSite.SHAPE_CLASS, Point.class.getName());
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
