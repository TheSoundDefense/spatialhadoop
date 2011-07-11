package edu.umn.cs.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.spatial.Point;

/**
 * Stores a point with an integer K.
 * This is used to form the query part of a KNN query.
 * @author aseldawy
 *
 */
public class PointWithK extends Point {
  /**
   * Generated UID
   */
  private static final long serialVersionUID = -4794848671765481819L;

  /**
   * K, number of nearest neighbors required to find
   */
  public int k;

  public PointWithK() {
  }
  
  public PointWithK(Point point, int k) {
    super(point.x, point.y);
    this.k = k;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(k);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.k = in.readInt();
  }
}
