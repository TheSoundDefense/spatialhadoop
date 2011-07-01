package edu.umn.edu.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.io.WritableComparable;

/**
 * A class that holds coordinates of a rectangle.
 * @author aseldawy
 *
 */
public class Rectangle implements WritableComparable<Rectangle>, Serializable, Cloneable {
  /**
   * Auto generated
   */
  private static final long serialVersionUID = 7801822896513739736L;

  public long id;
  public int x1;
  public int x2;
  public int y1;
  public int y2;
  public int type;

  public Rectangle() {
    this.set(0, 0, 0, 0, 0);
  }
  public int midX(){
    return (x1+x2)/2;
  }
  public int midY(){
    return (y1+y2)/2;
  }

  public Rectangle(long id, int x1, int y1, int x2, int y2) {
    int xl = Math.min(x1, x2);
    int xu = Math.max(x1, x2);
    int yl = Math.min(y1, y2);
    int yu = Math.max(y1, y2);

    this.set(id, xl, yl, xu, yu);
  }

  public void set(long id, int x1, int y1, int x2, int y2) {
    this.id = id;
    this.x1 = x1;
    this.y1 = y1;
    this.x2 = x2;
    this.y2 = y2;
  }

  public boolean intersects(Rectangle r2) {
    return this.x1 < r2.x2 && r2.x1 < this.x2 &&
      this.y1 < r2.y2 && r2.y1 < this.y2;
  }
  public int getXlower() {return x1;}
  public int getXupper() {return x2;}
  public int getYlower() {return y1;}
  public int getYupper() {return y2;}
  public long getId() {return id;}

  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    out.writeInt(x1);
    out.writeInt(y1);
    out.writeInt(x2);
    out.writeInt(y2);
    out.writeInt(this.type);
  }

  public void readFields(DataInput in) throws IOException {
    this.id = in.readLong();
    this.x1 = in.readInt();
    this.y1 = in.readInt();
    this.x2 = in.readInt();
    this.y2 = in.readInt();
    this.type = in.readInt();
  }

  /**
   * Comparison is done by lexicographic ordering of attributes
   * < x1, y2, x2, y2>
   */
  public int compareTo(Rectangle rect2) {
    // Sort by id
    int difference = this.x1 - rect2.x1;
    if (difference == 0) {
      difference = this.y1 - rect2.y1;
      if (difference == 0) {
        difference = this.x2 - rect2.x2;
        if (difference == 0) {
          difference = this.y2 - rect2.y2;
        }
      }
    }
    return difference;
  }

  public boolean equals(Object obj) {
    Rectangle r2 = (Rectangle) obj;
    boolean result = this.x1 == r2.x1 && this.x2 == r2.x2 && this.y1 == r2.y1 && this.y2 == r2.y2;
    return result;
  }

  @Override
  public int hashCode() {
    return x1+y1+x2+y2;
  }

  public String toString() {
    return "Rectangle #"+id+" ("+x1+","+y1+","+x2+","+y2+")";
  }
  public double distanceTo(Rectangle s) {
    double dx = s.x1 - this.x1;
    double dy = s.y1 - this.y1;
    return dx*dx+dy*dy;
  }

  public boolean contains(Point queryPoint) {
    return queryPoint.x >= this.x1 && queryPoint.x <= this.x2 &&
    queryPoint.y >= this.y1 && queryPoint.y <= this.y2;
  }

  public double maxDistance(Point point) {
    double dx = Math.max(point.x - this.x1, this.x2 - point.x);
    double dy = Math.max(point.y - this.y1, this.y2 - point.y);

    return Math.sqrt(dx*dx+dy*dy);
  }

  public double minDistance(Point point) {
    double dx = Math.min(point.x - this.x1, this.x2 - point.x);
    double dy = Math.min(point.y - this.y1, this.y2 - point.y);

    return Math.min(dx, dy);
  }

  @Override
  public Object clone() {
    Rectangle rect2 = new Rectangle(this.id, this.x1, this.y1, this.x2, this.y2);
    rect2.type = this.type;
    return rect2;
  }
  
  public Rectangle union(Rectangle rect2) {
    Rectangle newOne = new Rectangle();
    newOne.x1 = Math.min(this.x1, rect2.x1);
    newOne.y1 = Math.min(this.y1, rect2.y1);
    newOne.x2 = Math.max(this.x2, rect2.x2);
    newOne.y2 = Math.max(this.y2, rect2.y2);
    return newOne;
  }
}
