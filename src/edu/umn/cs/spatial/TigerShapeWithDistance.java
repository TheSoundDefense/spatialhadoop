package edu.umn.cs.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

public class TigerShapeWithDistance extends TigerShape {
  // Distance is used for internal processing and should not be serialized to string to
  // be able to parse normal files
  public double distance;

  public TigerShapeWithDistance() {
    
  }
  
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
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeDouble(distance);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    distance = in.readDouble();
  }
  
  @Override
  public String toString() {
    return super.toString() + " distance: "+distance;
  }
  
}
