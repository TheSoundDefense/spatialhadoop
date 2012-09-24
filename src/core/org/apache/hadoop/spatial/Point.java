package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Writable;

/**
 * A class that holds coordinates of a point.
 * @author aseldawy
 *
 */
public class Point extends AbstractShape implements Writable, Cloneable {
	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = 7801822896513739736L;
	
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
	public Object clone() {
	  return new Point(this.x, this.y);
	}

  @Override
  public Rectangle getMBR() {
    return new Rectangle(x, y, 1, 1);
  }

  @Override
  public double getMinDistanceTo(Shape s) {
    return distanceTo(s.getCenterPoint());
  }

  @Override
  public double getMaxDistanceTo(Shape s) {
    return distanceTo(s.getCenterPoint());
  }

  @Override
  public Shape getIntersection(Shape s) {
    return getMBR().getIntersection(s);
  }

  @Override
  public boolean isIntersected(Shape s) {
    return getMBR().isIntersected(s);
  }

  @Override
  public Shape union(Shape s) {
    return getMBR().union(s);
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
  public String writeToString() {
    return String.format("%s%x,%s%x", x < 0 ? "-" : "", Math.abs(x),
        y < 0 ? "-" : "", Math.abs(y));

  }
  
  @Override
  public void readFromString(String s) {
    String[] parts = s.split(",");
    this.x = Long.parseLong(parts[0], 16);
    this.y = Long.parseLong(parts[1], 16);
  }

  @Override
  public String toString() {
    return "Point: ("+x+","+y+")";
  }
  
  @Override
  public Point getCenterPoint() {
    return this;
  }

  @Override
  public void toText(Text text) {
    TextSerializerHelper.serializeLong(x, text, true);
    TextSerializerHelper.serializeLong(y, text, false);
  }
  
  @Override
  public void fromText(Text text) {
    final byte[] buffer = text.getBytes();
    int comma = 0;
    while (buffer[comma] != ',')
      comma++;
    x = TextSerializerHelper.deserializeLong(buffer, 0, comma++);
    y = TextSerializerHelper.deserializeLong(buffer, comma,
        text.getLength() - comma);
  }
}
