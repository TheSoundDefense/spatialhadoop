package org.apache.hadoop.spatial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
  
  /**Configuration line to set the default shape class to use if not set*/
  public static final String SHAPE_CLASS =
      "edu.umn.cs.spatialHadoop.ShapeRecordReader.ShapeClass.default";
  
  /**Whether or not to combine splits automatically to reduce map tasks*/
  public static final String AutoCombineSplits =
      "spatialHadoop.mapreduce.autoCombineSplits";
  
  /**Configuration line name for replication overhead*/
  public static final String REPLICATION_OVERHEAD =
      "spatialHadoop.storage.ReplicationOverHead";
  
  /**
   * A marker put in the beginning of each block to indicate that this block
   * is stored as an RTree. It might be better to store this in the BlockInfo
   * in a field (e.g. localIndexType).
   */
  public static final long RTreeFileMarker = -0x00012345678910L;
  public static byte[] RTreeFileMarkerB;
  
  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    try {
      dout.writeLong(RTreeFileMarker);
      dout.close();
      bout.close();
      RTreeFileMarkerB = bout.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
}
