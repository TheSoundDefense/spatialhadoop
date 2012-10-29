package edu.umn.cs.spatialHadoop;

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
  
  static {
    // Load configuration from files
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
}
