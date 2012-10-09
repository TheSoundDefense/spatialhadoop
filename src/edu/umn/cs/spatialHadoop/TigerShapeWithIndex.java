package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.spatial.TigerShape;

public class TigerShapeWithIndex extends TigerShape {
  
  /**
   * Note: index should not be written or read from string to be able to parse
   * normal files
   */
  public int index;

  public TigerShapeWithIndex() {
    super();
  }
  
  public TigerShapeWithIndex(TigerShapeWithIndex x) {
    this(x, x.index);
  }

  public TigerShapeWithIndex(TigerShape tigerShape, int index) {
    super(tigerShape);
    this.index = index;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(index);
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.index = in.readInt();
    super.readFields(in);
  }

  @Override
  public TigerShapeWithIndex clone() {
    return new TigerShapeWithIndex(this);
  }
  
  @Override
  public String writeToString() {
    return String.format("%s#%s%x", super.writeToString(), this.index < 0 ? "-" : "", Math.abs(index));
  }
  
  @Override
  public void readFromString(String s) {
    String[] parts = s.split("#", 2);
    super.readFromString(parts[0]);
    if (parts.length == 2) {
      this.index = Integer.parseInt(parts[1], 16);
    }
  }
  
}
