package org.apache.hadoop.spatial;

import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.Writable;

/**
 * A general 2D shape.
 * @author aseldawy
 *
 */
public interface Shape extends Writable, Cloneable, TextSerializable {
  /**
   * Returns minimum bounding rectangle for this shape.
   * @return
   */
  public Rectangle getMBR();
  
  /**
   * Gets the distance of this shape to the given point.
   * @param x
   * @param y
   * @return
   */
  public double distanceTo(long x, long y);
  
  /**
   * Returns true if this shape is intersected with the given shape
   * @param s
   * @return
   */
  public boolean isIntersected(final Shape s);
  
  /**
   * Returns a clone of this shape
   * @return
   * @throws CloneNotSupportedException
   */
  public Shape clone();
}
