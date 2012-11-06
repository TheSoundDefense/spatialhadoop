package edu.umn.cs.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;

/**
 * A shape from tiger file.
 * @author aseldawy
 *
 */
public class TigerShape extends Rectangle {
  
  public long id;

  public TigerShape() {
  }
  
  public TigerShape(Rectangle rect, long id) {
    super(rect);
    this.id = id;
  }

  public TigerShape(TigerShape tigerShape) {
    super(tigerShape);
    this.id = tigerShape.id;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    super.readFields(in);
  }

  @Override
  public int compareTo(Shape s) {
    return (int)(id - ((TigerShape)s).id);
  }

  @Override
  public TigerShape clone() {
    return new TigerShape(this);
  }
  
  @Override
  public String toString() {
    return String.format("TIGER #%x %s", id, super.toString());
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, ',');
    return super.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    this.id = TextSerializerHelper.consumeLong(text, ',');
    super.fromText(text);
  }
}
