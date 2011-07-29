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
  public void fromBuffer(byte[] buf, int start, int length) {
    int i2 = start + length - 1;
    while (buf[i2] != ',') i2--;
    this.k = (int) TextSerializerHelper.deserializeLong(buf, i2+1, start + length - (i2+1));
    super.fromBuffer(buf, start, i2 - start );
  }
  
  @Override
  public void toText(Text text) {
    super.toText(text);
    text.append(new byte[] {','}, 0, 1);
    TextSerializerHelper.serializeLong(k, text, false);
  }
}
