package edu.umn.edu.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

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
	
	public int id;
	public float x1;
	public float x2;
	public float y1;
	public float y2;
	public int type;

	public Rectangle() {
		this.set(0, 0.0f, 0.0f, 0.0f, 0.0f);
	}
	public float midX(){
		return (x1+x2)/2;
	}
	public float midY(){
		return (y1+y2)/2;
	}
	
	public Rectangle(int id, float x1, float y1, float x2, float y2) {
	   float xl = Math.min(x1, x2);
	   float xu = Math.max(x1, x2);
	   float yl = Math.min(y1, y2);
	   float yu = Math.max(y1, y2);
	   
		this.set(id, xl, yl, xu, yu);
	}

	public void set(int id, float x1, float y1, float x2, float y2) {
		this.id = id;
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
	}

	public boolean intersects(Rectangle r2) {
		return !(this.x2 < r2.x1 || r2.x2 < this.x1) &&
				!(this.y2 < r2.y1 || r2.y2 < this.y1);
	}
	float getXlower() {return x1;}
	float getXupper() {return x2;}
	float getYlower() {return y1;}
	float getYupper() {return y2;}
	int getId() {return id;}
	public static void main(String[] args) {
		Rectangle r1 = new Rectangle(1, 0,0, 100, 100);
		Rectangle r2 = new Rectangle(1, 0,0, 50, 50);
		Rectangle r3 = new Rectangle(1, -100,-100, -50, -50);
		Rectangle r4 = new Rectangle(1, -100,0, -50, 50);
		if (!r1.intersects(r2))
			System.err.println("error 1");
		if (r1.intersects(r3))
			System.err.println("error 2");
		if (r1.intersects(r4))
			System.err.println("error 3");
		System.out.println("Rectangle success");
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeFloat(x1);
		out.writeFloat(y1);
		out.writeFloat(x2);
		out.writeFloat(y2);
		out.writeInt(this.type);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.x1 = in.readFloat();
		this.y1 = in.readFloat();
		this.x2 = in.readFloat();
		this.y2 = in.readFloat();
		this.type = in.readInt();
	}

	/**
	 * Comparison is done by lexicographic ordering of attributes
	 * < x1, y2, x2, y2>
	 */
	public int compareTo(Rectangle rect2) {
		// Sort by id
		double difference = this.x1 - rect2.x1;
		if (difference == 0.0) difference = this.y1 - rect2.y1;
	  if (difference == 0.0) difference = this.x2 - rect2.x2;
	  if (difference == 0.0) difference = this.y2 - rect2.y2;
	  if (difference < 0)
	    return -1;
	  if (difference > 0)
	    return 1;
		return 0;

	}
	
	public boolean equals(Object obj) {
		Rectangle r2 = (Rectangle) obj;
		boolean result = this.x1 == r2.x1 && this.x2 == r2.x2 && this.y1 == r2.y1 && this.y2 == r2.y2;
		return result;
	}
	
	@Override
	public int hashCode() {
	  return new Float(x1+y1+x2+y2).hashCode();
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
}
