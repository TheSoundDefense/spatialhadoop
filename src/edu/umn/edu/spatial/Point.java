package edu.umn.edu.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

/**
 * A class that holds coordinates of a point.
 * @author aseldawy
 *
 */
public class Point implements WritableComparable<Point>, Serializable, Cloneable {
	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = 7801822896513739736L;
	
	public long id;
	public int x;
	public int y;
	public int type;

	public Point() {
		this(0, 0);
	}
	
	public Point(int x, int y) {
	  this(0, x, y);
	}
	
	public Point(long id, int x, int y) {
		this(id, x, y, 0);
	}
	
	/**
	 * @param id
	 * @param x
	 * @param y
	 * @param type
	 */
	public Point(long id, int x, int y, int type) {
	  set(id, x, y, type);
	}

	public void set(long id, int x, int y, int type) {
		this.id = id;
		this.x = x;
		this.y = y;
	}

	int getX() {return x;}
	int getY() {return y;}
	long getId() {return id;}

	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeInt(x);
		out.writeInt(y);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		this.x = in.readInt();
		this.y = in.readInt();
	}

	public int compareTo(Point pt2) {
		// Sort by id
		int difference = this.x - pt2.x;
		if (difference == 0) {
			difference = this.y - pt2.y;
		}
		return difference;

	}
	
	public boolean equals(Object obj) {
		Point r2 = (Point) obj;
		return this.x == r2.x && this.y == r2.y;
	}
	
	public String toString() {
		return "Point #"+id+" ("+x+","+y+")";
	}

	public double distanceTo(Point s) {
		double dx = s.x - this.x;
		double dy = s.y - this.y;
		return dx*dx+dy*dy;
	}
	
	@Override
	public Object clone() {
	  return new Point(this.id, this.x, this.y, this.type);
	}
}
