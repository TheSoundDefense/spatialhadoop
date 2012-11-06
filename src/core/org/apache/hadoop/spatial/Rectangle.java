package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;

/**
 * A class that holds coordinates of a rectangle. For predicate test functions
 * (e.g. intersection), the rectangle is considered open-ended. This means that
 * the right and top edge are outside the rectangle.
 * @author aseldawy
 *
 */
public class Rectangle implements Shape {
  public long x;
  public long y;
  public long width;
  public long height;

  public Rectangle() {
    this(0, 0, 0, 0);
  }

  /**
   * Constructs a new <code>Rectangle</code>, initialized to match 
   * the values of the specified <code>Rectangle</code>.
   * @param r  the <code>Rectangle</code> from which to copy initial values
   *           to a newly constructed <code>Rectangle</code>
   * @since 1.1
   */
  public Rectangle(Rectangle r) {
    this(r.x, r.y, r.width, r.height);
  }

  public Rectangle(long x, long y, long width, long height) {
    this.set(x, y, width, height);
  }
  
  public void set(long x, long y, long width, long height) {
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(x);
    out.writeLong(y);
    out.writeLong(width);
    out.writeLong(height);
  }

  public void readFields(DataInput in) throws IOException {
    this.x = in.readLong();
    this.y = in.readLong();
    this.width = in.readLong();
    this.height = in.readLong();
  }
  
  public long getX1() { return x; }
  /**
   * X2 is considered inside the polygon.
   * @return
   */
  public long getX2() { return x + width; }
  public long getXMid() { return x + width / 2; }
  public long getY1() { return y; }
  public long getY2() { return y + height; }
  public long getYMid() { return y + height / 2; }

  /**
   * Comparison is done by lexicographic ordering of attributes
   * < x1, y2, x2, y2>
   */
  public int compareTo(Shape s) {
    Rectangle rect2 = (Rectangle) s;
    // Sort by id
    long difference = this.x - rect2.x;
    if (difference == 0) {
      difference = this.y - rect2.y;
      if (difference == 0) {
        difference = this.width - rect2.width;
        if (difference == 0) {
          difference = this.height - rect2.height;
        }
      }
    }
    return (int)difference;
  }

  public boolean equals(Object obj) {
    Rectangle r2 = (Rectangle) obj;
    boolean result = this.x == r2.x && this.y == r2.y && this.width == r2.width && this.height == r2.height;
    return result;
  }

  @Override
  public double distanceTo(long px, long py) {
    return this.getMaxDistanceTo(px, py);
  }

  public double getMaxDistanceTo(long px, long py) {
    double dx = Math.max(px - this.x, this.x + this.width - px);
    double dy = Math.max(py - this.y, this.y + this.height - py);

    return Math.sqrt(dx*dx+dy*dy);
  }

  public double getMinDistanceTo(long px, long py) {
    if (this.contains(px, py))
      return 0;
    
    long dx = Math.min(Math.abs(px - this.x), Math.abs(this.x + this.width - px));
    long dy = Math.min(Math.abs(py - this.y), Math.abs(this.y + this.height - py));

    if ((px < this.x || px > this.x + this.width) &&
        (py < this.y || py > this.y + this.height)) {
      return Math.sqrt((double)dx * dx + (double)dy * dy);
    }
    
    return Math.min(dx, dy);
  }

  @Override
  public Rectangle clone() {
    return new Rectangle(this);
  }
  
  @Override
  public Rectangle getMBR() {
    return this;
  }
  
  /** All next functions are copied with slight modifications from java.awt.Rectangle */
  /**
   * Determines whether or not this <code>Rectangle</code> and the specified 
   * <code>Rectangle</code> intersect. Two rectangles intersect if 
   * their intersection is nonempty. 
   *
   * @param r the specified <code>Rectangle</code>
   * @return    <code>true</code> if the specified <code>Rectangle</code> 
   *            and this <code>Rectangle</code> intersect; 
   *            <code>false</code> otherwise.
   */
  public boolean isIntersected(Shape s) {
    if (s instanceof Point) {
      Point pt = (Point)s;
      return pt.x >= x && pt.x < x + width && pt.y >= y && pt.y < y + height;
    }
    Rectangle r = s.getMBR();
    return (this.getX2() > r.getX1() && r.getX2() > this.getX1() &&
        this.getY2() > r.getY1() && r.getY2() > this.getY1());
  }

  /**
   * Computes the intersection of this <code>Rectangle</code> with the 
   * specified <code>Rectangle</code>. Returns a new <code>Rectangle</code> 
   * that represents the intersection of the two rectangles.
   * If the two rectangles do not intersect, the result will be
   * an empty rectangle.
   *
   * @param     r   the specified <code>Rectangle</code>
   * @return    the largest <code>Rectangle</code> contained in both the 
   *            specified <code>Rectangle</code> and in 
   *      this <code>Rectangle</code>; or if the rectangles
   *            do not intersect, an empty rectangle.
   */
  public Shape getIntersection(Shape s) {
    Rectangle r = s.getMBR();
    long tx1 = this.x;
    long ty1 = this.y;
    long rx1 = r.x;
    long ry1 = r.y;
    long tx2 = tx1; tx2 += this.width;
    long ty2 = ty1; ty2 += this.height;
    long rx2 = rx1; rx2 += r.width;
    long ry2 = ry1; ry2 += r.height;
    if (tx1 < rx1) tx1 = rx1;
    if (ty1 < ry1) ty1 = ry1;
    if (tx2 > rx2) tx2 = rx2;
    if (ty2 > ry2) ty2 = ry2;
    tx2 -= tx1;
    ty2 -= ty1;
    // tx2,ty2 will never overflow (they will never be
    // larger than the smallest of the two source w,h)
    // they might underflow, though...
    if (tx2 < Long.MIN_VALUE) tx2 = Long.MIN_VALUE;
    if (ty2 < Long.MIN_VALUE) ty2 = Long.MIN_VALUE;
    return new Rectangle(tx1, ty1, (int) tx2, (int) ty2);
  }

  /**
   * Returns the location of this <code>Rectangle</code>.
   * <p>
   * This method is included for completeness, to parallel the
   * <code>getLocation</code> method of <code>Component</code>.
   * @return the <code>Point</code> that is the upper-left corner of
   *      this <code>Rectangle</code>. 
   * @see       java.awt.Component#getLocation
   * @see       #setLocation(Point)
   * @see       #setLocation(int, int)
   * @since     1.1
   */
  public Point getLocation() {
    return new Point(x, y);
  }

  /**
   * Checks whether or not this <code>Rectangle</code> contains the 
   * specified <code>Point</code>.
   * @param p the <code>Point</code> to test
   * @return    <code>true</code> if the specified <code>Point</code> 
   *            is inside this <code>Rectangle</code>; 
   *            <code>false</code> otherwise.
   * @since     1.1
   */
  public boolean contains(Point p) {
    return contains(p.x, p.y);
  }

  /**
   * Checks whether or not this <code>Rectangle</code> contains the 
   * point at the specified location {@code (x,y)}.
   *
   * @param  x the specified X coordinate
   * @param  y the specified Y coordinate
   * @return    <code>true</code> if the point 
   *            {@code (x,y)} is inside this 
   *      <code>Rectangle</code>; 
   *            <code>false</code> otherwise.
   * @since     1.1
   */
  public boolean contains(long X, long Y) {
    long w = this.width;
    long h = this.height;
    if ((w | h) < 0) {
      // At least one of the dimensions is negative...
      return false;
    }
    // Note: if either dimension is zero, tests below must return false...
    long x = this.x;
    long y = this.y;
    if (X < x || Y < y) {
      return false;
    }
    w += x;
    h += y;
    //    overflow || intersect
    return ((w < x || w > X) &&
        (h < y || h > Y));
  }

  /**
   * Checks whether or not this <code>Rectangle</code> entirely contains 
   * the specified <code>Rectangle</code>.
   *
   * @param     r   the specified <code>Rectangle</code>
   * @return    <code>true</code> if the <code>Rectangle</code> 
   *            is contained entirely inside this <code>Rectangle</code>; 
   *            <code>false</code> otherwise
   * @since     1.2
   */
  public boolean contains(Rectangle r) {
    return contains(r.x, r.y, r.width, r.height);
  }

  /**
   * Computes the union of this <code>Rectangle</code> with the 
   * specified <code>Rectangle</code>. Returns a new 
   * <code>Rectangle</code> that 
   * represents the union of the two rectangles.
   * <p>
   * If either {@code Rectangle} has any dimension less than zero
   * the rules for <a href=#NonExistant>non-existant</a> rectangles
   * apply.
   * If only one has a dimension less than zero, then the result
   * will be a copy of the other {@code Rectangle}.
   * If both have dimension less than zero, then the result will
   * have at least one dimension less than zero.
   * <p>
   * If the resulting {@code Rectangle} would have a dimension
   * too large to be expressed as an {@code int}, the result
   * will have a dimension of {@code Long.MAX_VALUE} along
   * that dimension.
   * @param r the specified <code>Rectangle</code>
   * @return    the smallest <code>Rectangle</code> containing both 
   *      the specified <code>Rectangle</code> and this 
   *      <code>Rectangle</code>.
   */
  public Shape union(final Shape s) {
    Rectangle r = s.getMBR();
    long tx2 = this.width;
    long ty2 = this.height;
    if ((tx2 | ty2) < 0) {
      // This rectangle has negative dimensions...
      // If r has non-negative dimensions then it is the answer.
      // If r is non-existant (has a negative dimension), then both
      // are non-existant and we can return any non-existant rectangle
      // as an answer.  Thus, returning r meets that criterion.
      // Either way, r is our answer.
      return new Rectangle(r);
    }
    long rx2 = r.width;
    long ry2 = r.height;
    if ((rx2 | ry2) < 0) {
      return new Rectangle(this);
    }
    long tx1 = this.x;
    long ty1 = this.y;
    tx2 += tx1;
    ty2 += ty1;
    long rx1 = r.x;
    long ry1 = r.y;
    rx2 += rx1;
    ry2 += ry1;
    if (tx1 > rx1) tx1 = rx1;
    if (ty1 > ry1) ty1 = ry1;
    if (tx2 < rx2) tx2 = rx2;
    if (ty2 < ry2) ty2 = ry2;
    tx2 -= tx1;
    ty2 -= ty1;
    // tx2,ty2 will never underflow since both original rectangles
    // were already proven to be non-empty
    // they might overflow, though...
    if (tx2 > Long.MAX_VALUE) tx2 = Long.MAX_VALUE;
    if (ty2 > Long.MAX_VALUE) ty2 = Long.MAX_VALUE;
    return new Rectangle(tx1, ty1, (int) tx2, (int) ty2);
  }
  
  /**
   * Checks whether this <code>Rectangle</code> entirely contains 
   * the <code>Rectangle</code>
   * at the specified location {@code (X,Y)} with the
   * specified dimensions {@code (W,H)}.
   * @param     X the specified X coordinate
   * @param     Y the specified Y coordinate
   * @param     W   the width of the <code>Rectangle</code>
   * @param     H   the height of the <code>Rectangle</code>
   * @return    <code>true</code> if the <code>Rectangle</code> specified by
   *            {@code (X, Y, W, H)}
   *            is entirely enclosed inside this <code>Rectangle</code>; 
   *            <code>false</code> otherwise.
   * @since     1.1
   */
  public boolean contains(long X, long Y, long W, long H) {
    long w = this.width;
    long h = this.height;
    if ((w | h | W | H) < 0) {
      // At least one of the dimensions is negative...
      return false;
    }
    // Note: if any dimension is zero, tests below must return false...
    long x = this.x;
    long y = this.y;
    if (X < x || Y < y) {
      return false;
    }
    w += x;
    W += X;
    if (W <= X) {
      // X+W overflowed or W was zero, return false if...
      // either original w or W was zero or
      // x+w did not overflow or
      // the overflowed x+w is smaller than the overflowed X+W
      if (w >= x || W > w) return false;
    } else {
      // X+W did not overflow and W was not zero, return false if...
      // original w was zero or
      // x+w did not overflow and x+w is smaller than X+W
      if (w >= x && W > w) return false;
    }
    h += y;
    H += Y;
    if (H <= Y) {
      if (h >= y || H > h) return false;
    } else {
      if (h >= y && H > h) return false;
    }
    return true;
  }
  
  public Point getCenterPoint() {
    return new Point(x + width / 2, y + height / 2);
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(x, text, ',');
    TextSerializerHelper.serializeLong(y, text, ',');
    TextSerializerHelper.serializeLong(width, text, ',');
    TextSerializerHelper.serializeLong(height, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    x = TextSerializerHelper.consumeLong(text, ',');
    y = TextSerializerHelper.consumeLong(text, ',');
    width = TextSerializerHelper.consumeLong(text, ',');
    height = TextSerializerHelper.consumeLong(text, '\0');
  }

  @Override
  public String toString() {
    return "Rectangle: ("+x+","+y+")-("+getX2()+","+getY2()+") "+width+"x"+height;
  }

}
