package org.apache.hadoop.spatial;

import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A general 2D shape.
 * @author aseldawy
 *
 */
public interface Shape extends WritableComparable<Shape>, Cloneable, TextSerializable {
  /**
   * Returns minimum bounding rectangle for this shape.
   * @return
   */
  public Rectangle getMBR();
  
  /**
   * Returns the center point for this shape.
   * @return
   */
  public Point getCenterPoint();
  
  /**
   * Return Centroid (similar to CenterPoint).
   * @return
   */
  public Point getCentroid();
  
  /**
   * Returns minimum distance to another shape
   * @return
   */
  public double getMinDistanceTo(final Shape s);
  
  /**
   * Returns the maximum distance to another shape
   * @param p
   * @return
   */
  public double getMaxDistanceTo(final Shape s);
  
  /**
   * Returns average distance to another shape
   * @param p
   * @return
   */
  public double getAvgDistanceTo(final Shape s);
  
  /**
   * Returns the intersection between this shape and another shape
   * @param s
   * @return
   */
  public Shape getIntersection(final Shape s);
  
  /**
   * Returns true if this shape is intersected with the given shape
   * @param s
   * @return
   */
  public boolean isIntersected(final Shape s);
  
  /**
   * Returns the union between this shape and another shape.
   * @param s
   * @return
   */
  public Shape union(final Shape s);

  /**
   * Returns a clone of this shape
   * @return
   * @throws CloneNotSupportedException
   */
  public Shape clone();
}
