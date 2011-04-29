package edu.umn.edu.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores a point with distance to a fixed query point.
 * Used as intermediate data for KNN.
 * @author aseldawy
 *
 */
public class PointWithDistance extends Point {
  /**
   * Generated UID
   */
  private static final long serialVersionUID = 6871432880431569309L;
  
  /**
   * Distance to a fixed query point
   */
  private double distance;

  public PointWithDistance() {
  }
  
  public PointWithDistance(Point point, double distance) {
    super(point.id, point.x, point.y);
    this.distance = distance;
  }

  public double getDistance() {
    return distance;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeDouble(distance);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.distance = in.readDouble();
  }
  
  @Override
  public Object clone() {
    return new PointWithDistance(this, this.distance);
  }
  
  @Override
  public String toString() {
    return super.toString() + " - " + this.distance;
  }
}
