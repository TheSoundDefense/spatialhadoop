package org.apache.hadoop.spatial;

import org.apache.hadoop.io.Text;

/**
 * An abstract implementation for Shape
 * @author aseldawy
 *
 */
public abstract class AbstractShape implements Shape {
  
  @Override
  public abstract AbstractShape clone();
  
  @Override
  public void fromText(Text text) {
    readFromString(text.toString());
  }
  
}
