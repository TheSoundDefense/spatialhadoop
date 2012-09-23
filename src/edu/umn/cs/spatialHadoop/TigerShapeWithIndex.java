package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

public class TigerShapeWithIndex extends TigerShape {
  
  // Note: index should not be written or read from string to be able to parse normal files
  public int index;

  public TigerShapeWithIndex() {
    super();
  }

  public TigerShapeWithIndex(Shape shape, long id) {
    super(shape, id);
  }

  public TigerShapeWithIndex(TigerShape tigerShape) {
    super(tigerShape);
  }

  public TigerShapeWithIndex(TigerShape tigerShape, int index) {
    super(tigerShape);
    this.index = index;
  }

  public TigerShapeWithIndex(Shape shape, long id, int index) {
    super(shape, id);
    this.index = index;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(index);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.index = in.readInt();
  }

  @Override
  public Object clone() {
    return new TigerShapeWithIndex((Shape) this.shape.clone(), this.id, this.index);
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
