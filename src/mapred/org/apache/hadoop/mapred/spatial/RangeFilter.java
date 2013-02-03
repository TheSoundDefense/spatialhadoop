package org.apache.hadoop.mapred.spatial;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.Shape;


public class RangeFilter extends DefaultBlockFilter {
  
  /**Name of the config line that stores the class name of the query shape*/
  private static final String QUERY_SHAPE_CLASS =
      "edu.umn.cs.spatialHadoop.mapReduce.RangeFilter.QueryShapeClass";

  /**Name of the config line that stores the query shape*/
  private static final String QUERY_SHAPE =
      "edu.umn.cs.spatialHadoop.mapReduce.RangeFilter.QueryShape";

  /**A shape that is used to filter input*/
  private Shape queryShape;
  
  /**
   * Sets the query range in the given job.
   * @param job
   * @param shape
   */
  public static void setQueryRange(JobConf job, Shape shape) {
    job.setClass(QUERY_SHAPE_CLASS, shape.getClass(), Shape.class);
    job.set(QUERY_SHAPE, shape.toText(new Text()).toString());
  }
  
  @Override
  public void configure(JobConf job) {
    super.configure(job);
    try {
      String queryShapeClassName = job.get(QUERY_SHAPE_CLASS);
      Class<? extends Shape> queryShapeClass =
          Class.forName(queryShapeClassName).asSubclass(Shape.class);
      queryShape = queryShapeClass.newInstance();
      queryShape.fromText(new Text(job.get(QUERY_SHAPE)));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public boolean processBlock(BlockLocation blk) {
    return blk.getCellInfo() == null || queryShape.isIntersected(blk.getCellInfo());
  }
}