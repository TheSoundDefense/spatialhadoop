package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

public class TigerShapeWithDistance extends TigerShape {
  
  /**
   * Distance is used for internal processing and should not be serialized
   * to string to be able to parse normal files
   */
  public double distance;

  public TigerShapeWithDistance() {
  }
  
  TigerShapeWithDistance(TigerShapeWithDistance x) {
    super(x);
    this.distance = x.distance;
  }
  
  public TigerShapeWithDistance(Shape shape, double distance) {
    super(shape.getMBR(), 0);
    this.distance = distance;
  }
  
  @Override
  public TigerShapeWithDistance clone() {
    return new TigerShapeWithDistance(this);
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
  
  @Override
  public String writeToString() {
    return String.format("%f;%s", this.distance, super.writeToString());
  }
  
  @Override
  public void readFromString(String s) {
    String[] parts = s.split(";", 2);
    if (parts.length > 1) {
      this.distance = Double.parseDouble(parts[0]);
      super.readFromString(parts[1]);
    } else {
      this.distance = 0.0;
      super.readFromString(parts[0]);
    }
  }
}
