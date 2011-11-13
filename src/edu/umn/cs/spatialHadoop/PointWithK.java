package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;
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

  public PointWithK(long x, long y, int k) {
    super(x, y);
    this.k = k;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(k);
  }
  
  @Override
  public String toString() {
    return String.format("%s K=%x", super.toString(), k);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.k = in.readInt();
  }
  
  @Override
  public String writeToString() {
    return String.format("%s%x,%s", this.k < 0 ? "-" : "", Math.abs(k),
        super.writeToString());
  }
  
  @Override
  public void readFromString(String s) {
    String[] parts = s.split(",", 2);
    this.k = Integer.parseInt(parts[0], 16);
    super.readFromString(parts[1]);
  }
}
