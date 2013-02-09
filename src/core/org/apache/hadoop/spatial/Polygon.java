package org.apache.hadoop.spatial;

import java.awt.geom.Rectangle2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializerHelper;

/**
 * A class that represents a polygon using a list of points.
 * @author eldawy
 *
 */
public class Polygon extends java.awt.Polygon implements Shape {

  private static final long serialVersionUID = -117491486038680078L;

  public Polygon() {
    super();
  }

  public Polygon(int[] xpoints, int[] ypoints, int npoints) {
    super(xpoints, ypoints, npoints);
  }
  
  /**
   * Set the points in the rectangle to the given array
   * @param xpoints
   * @param ypoints
   * @param npoints
   */
  public void set(int[] xpoints, int[] ypoints, int npoints) {
    this.npoints = npoints;
    this.xpoints = new int[npoints];
    this.ypoints = new int[npoints];
    System.arraycopy(xpoints, 0, this.xpoints, 0, npoints);
    System.arraycopy(ypoints, 0, this.ypoints, 0, npoints);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(npoints);
    for (int i = 0; i < npoints; i++) {
      out.writeInt(xpoints[i]);
      out.writeInt(ypoints[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.npoints = in.readInt();
    this.xpoints = new int[npoints];
    this.ypoints = new int[npoints];
    
    for (int i = 0; i < npoints; i++) {
      this.xpoints[i] = in.readInt();
      this.ypoints[i] = in.readInt();
    }
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeInt(npoints, text, ',');
    for (int i = 0; i < npoints; i++) {
      TextSerializerHelper.serializeInt(xpoints[i], text, ',');
      TextSerializerHelper.serializeInt(ypoints[i], text,
          i == npoints - 1 ? '\0' : ',');
    }
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.npoints = TextSerializerHelper.consumeInt(text, ',');
    this.xpoints = new int[npoints];
    this.ypoints = new int[npoints];
    
    for (int i = 0; i < npoints; i++) {
      this.xpoints[i] = TextSerializerHelper.consumeInt(text, ',');
      this.ypoints[i] = TextSerializerHelper.consumeInt(text,
          i == npoints - 1 ? '\0' : ',');
    }
  }

  @Override
  public Rectangle getMBR() {
    Rectangle2D mbr = super.getBounds2D();
    return new Rectangle((long)mbr.getMinX(), (long)mbr.getMinY(),
        (long)mbr.getWidth(), (long)mbr.getHeight());
  }

  @Override
  public double distanceTo(long x, long y) {
    double dx = x - getBounds2D().getCenterX();
    double dy = y - getBounds2D().getCenterY();
    return Math.sqrt(dx * dx + dy * dy);
  }

  @Override
  public boolean isIntersected(Shape s) {
    Rectangle2D mbr = super.getBounds2D();
    return super.intersects(mbr.getMinX(), mbr.getMinY(),
        mbr.getWidth(), mbr.getHeight());
  }
  
  public Polygon clone() {
    return new Polygon(xpoints, ypoints, npoints);
  }

}
