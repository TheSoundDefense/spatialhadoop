package org.apache.hadoop.spatial;

import org.apache.hadoop.conf.Configuration;

/**
 * Combines all the configuration needed for SpatialHadoop.
 * @author eldawy
 *
 */
public class SpatialSite {

  /**Enforce static only calls*/
  private SpatialSite() {}
  
  /**The class used to filter blocks before starting map tasks*/
  public static final String FilterClass = "spatialHadoop.mapreduce.filter";
  
  /**The default RTree degree used for local indexing*/
  public static final String RTREE_DEGREE = "spatialHadoop.storage.RTreeDegree";
  
  /**Maximum size of an RTree.*/
  public static final String RTREE_BLOCK_SIZE =
      "spatialHadoop.storage.RTreeBlockSize";
  
  /**Whether to build the RTree in fast mode or slow (memory saving) mode.*/
  public static final String RTREE_BUILD_MODE =
      "spatialHadoop.storage.RTreeBuiltMode";
  
  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
}
