package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;

/**
 * A class that holds coordinates of a point.
 * @author aseldawy
 *
 */
public class Point implements Shape {
	public long x;
	public long y;

	public Point() {
		this(0, 0);
	}
	
	public Point(long x, long y) {
	  set(x, y);
	}
	

	public void set(long x, long y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(x);
		out.writeLong(y);
	}

	public void readFields(DataInput in) throws IOException {
		this.x = in.readLong();
		this.y = in.readLong();
	}

	public int compareTo(Shape s) {
	  Point pt2 = (Point) s;

	  // Sort by id
		long difference = this.x - pt2.x;
		if (difference == 0) {
			difference = this.y - pt2.y;
		}
		return (int)difference;

	}
	
	public boolean equals(Object obj) {
		Point r2 = (Point) obj;
		return this.x == r2.x && this.y == r2.y;
	}
	
	public double distanceTo(Point s) {
		double dx = s.x - this.x;
		double dy = s.y - this.y;
		return Math.sqrt(dx*dx+dy*dy);
	}
	
	@Override
	public Point clone() {
	  return new Point(this.x, this.y);
	}

  @Override
  public Rectangle getMBR() {
    return new Rectangle(x, y, 1, 1);
  }

  @Override
  public double distanceTo(long px, long py) {
    long dx = x - px;
    long dy = y - py;
    return Math.sqrt((double)dx * dx + (double) dy * dy);
  }

  public Shape getIntersection(Shape s) {
    return getMBR().getIntersection(s);
  }

  @Override
  public boolean isIntersected(Shape s) {
    return getMBR().isIntersected(s);
  }

  public long mortonOrder() {
    return Point.mortonOrder(x, y);
  }
  
  public static long mortonOrder(long x, long y) {
    return sparsify(x) | (sparsify(y) << 1);
  }
  
  /**
   * Sparses a number making it ready to be interleaved with another number
   * @param x
   * @return
   */
  public static long sparsify(long x) {
    long sparsified = 0;
    for (int i = 0; x != 0 && i < 32; i++) {
      sparsified |= ((x & 1) << i * 2);
      x >>= 1;
    }
    return sparsified;
  }

  @Override
  public String toString() {
    return "Point: ("+x+","+y+")";
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(x, text, ',');
    TextSerializerHelper.serializeLong(y, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    x = TextSerializerHelper.consumeLong(text, ',');
    y = TextSerializerHelper.consumeLong(text, '\0');
  }

}
