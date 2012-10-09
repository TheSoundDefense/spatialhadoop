package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;

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
  public void toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, ';');
    super.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    byte[] buf = text.getBytes();
    int separator = 0;
    while (buf[separator] != ';')
      separator++;
    id = TextSerializerHelper.deserializeLong(buf, 0, separator++);
    text.set(buf, separator, text.getLength() - separator);
    super.fromText(text);
  }

  @Override
  public String writeToString() {
    return String.format("%s%x;%s", id < 0 ? "-" : "", Math.abs(id), super.writeToString());
  }
  
  @Override
  public void readFromString(String s) {
    String[] parts = s.split(";", 2);
    this.id = Long.parseLong(parts[0], 16);
    super.readFromString(parts[1]);
  }

}
