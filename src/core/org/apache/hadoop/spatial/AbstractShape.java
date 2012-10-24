package org.apache.hadoop.spatial;

import org.apache.hadoop.io.Text;

/**
 * An abstract implementation for Shape
 * @author aseldawy
 *
 */
public abstract class AbstractShape implements Shape {

  /**
   * A default implementation that returns the center point of the MBR
   */
  public Point getCenterPoint() {
    return getMBR().getCenterPoint();
  }
  
  @Override
  public abstract AbstractShape clone();
  
  @Override
  public void fromText(Text text) {
    readFromString(text.toString());
  }
  
}
