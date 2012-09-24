package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
  @Override
  public Point getCenterPoint() {
    return getMBR().getCenterPoint();
  }
  
  /**
   * A default implementation that returns getCenterPoint()
   */
  @Override
  public Point getCentroid() {
    return getCenterPoint();
  }

  /**
   * A default implementation that returns the average between minDistance and maxDistance
   */
  @Override
  public double getAvgDistanceTo(final Shape s) {
    return (getMinDistanceTo(s) + getMaxDistanceTo(s)) / 2.0;
  }

  @Override
  public abstract Object clone();
  
  @Override
  public void fromText(Text text) {
    readFromString(text.toString());
  }
  
}
