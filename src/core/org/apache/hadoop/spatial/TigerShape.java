package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;

/**
 * A shape from tiger file.
 * This is a wrapper to any shape and delegate all method to shape.
 * @author aseldawy
 *
 */
public class TigerShape implements Shape {
  
  /**
   * Maps each class ID to a sample object from this class.
   * This (stock) object can be used to deserialize an object from disk
   * without the need to create a new object each time.
   */
  private static final Map<Character, Shape> ClassID2Object =
      new HashMap<Character, Shape>();
  private static final Map<Class<? extends Shape>, Character> Class2ID =
      new HashMap<Class<? extends Shape>, Character>();
  
  static {
    ClassID2Object.put('p', new Point());
    Class2ID.put(Point.class, 'p');
    ClassID2Object.put('r', new Rectangle());
    Class2ID.put(Rectangle.class, 'r');
  }
  
  public Shape shape;
  public long id;

  public TigerShape() {
  }
  
  public TigerShape(Shape shape, long id) {
    this.shape = shape;
    this.id = id;
  }

  public TigerShape(TigerShape tigerShape) {
    this.id = tigerShape.id;
    this.shape = tigerShape.shape;
  }

  public Rectangle getMBR() {
    return shape.getMBR();
  }

  public Point getCenterPoint() {
    return shape.getCenterPoint();
  }

  public Point getCentroid() {
    return shape.getCentroid();
  }

  public double getMinDistanceTo(Shape s) {
    return shape.getMinDistanceTo(s);
  }

  public double getMaxDistanceTo(Shape s) {
    return shape.getMaxDistanceTo(s);
  }

  public double getAvgDistanceTo(Shape s) {
    return shape.getAvgDistanceTo(s);
  }

  public Shape getIntersection(Shape s) {
    return shape.getIntersection(s);
  }

  public boolean isIntersected(Shape s) {
    return shape.isIntersected(s);
  }

  public Shape union(Shape s) {
    return shape.union(s);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    out.writeChar(Class2ID.get(shape.getClass()));
    shape.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    shape = ClassID2Object.get(in.readChar());
    shape.readFields(in);
  }

  @Override
  public int compareTo(Shape s) {
    Shape shape = s instanceof TigerShape? ((TigerShape)s).shape : s;
    return shape.compareTo(shape);
  }

  @Override
  public Object clone() {
    return new TigerShape((Shape) shape.clone(), id);
  }
  
  @Override
  public String toString() {
    return String.format("TIGER #%x %s", id, shape.toString());
  }

  @Override
  public void toText(Text text) {
    TextSerializerHelper.serializeLong(id, text, true);
    shape.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    byte[] buf = text.getBytes();
    int comma = 0;
    while (buf[comma] != ',')
      comma++;
    id = TextSerializerHelper.deserializeLong(buf, 0, comma++);
    text.set(buf, comma, text.getLength() - comma);
    shape.fromText(text);
  }

  @Override
  public String writeToString() {
    return String.format("%s%x,%s", id < 0 ? "-" : "", Math.abs(id), shape.writeToString());
  }
  
  @Override
  public void readFromString(String s) {
    try {
    String[] parts = s.split(",", 2);
    this.id = Long.parseLong(parts[0], 16);
    shape.readFromString(parts[1]);
    } catch (Exception e) {
      e.printStackTrace();
      throw(new RuntimeException("asdfasd"));
    }
  }

}
