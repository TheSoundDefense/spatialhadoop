package edu.umn.cs.spatial;

import org.apache.hadoop.spatial.Shape;

public class TigerShapeWithDistance extends TigerShape {
  public double distance;
  
  public TigerShapeWithDistance(TigerShape tigerShape, double distance) {
    super(tigerShape);
    this.distance = distance;
  }
  
  public TigerShapeWithDistance(long id, Shape shape, double distance) {
    this.id = id;
    this.shape = shape;
    this.distance = distance;
  }
  
  @Override
  public Object clone() {
    return new TigerShapeWithDistance(id, (Shape) shape.clone(), distance);
  }
  
  @Override
  public int compareTo(Shape s) {
    TigerShapeWithDistance tswd = (TigerShapeWithDistance) s;
    double difference = distance - tswd.distance;
    if (difference < 0)
      return -1;
    if (difference > 0)
      return 1;
    return 0;
  }
}
