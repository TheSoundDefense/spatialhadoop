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
public class Point implements WritableComparable<Point>, Serializable {
	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = 7801822896513739736L;
	
	public int id;
	public int type;
	public float x;
	public float y;

	public Point() {
		this.set(0, 0.0f, 0.0f);
	}
	
	public Point(int id, float x, float y) {
		this.set(id, x, y);
	}
	
	public Point(float x, float y) {
		this(0, x, y);
	}

	public void set(int id, float x, float y) {
		this.id = id;
		this.x = x;
		this.y = y;
	}

	float getX() {return x;}
	float getY() {return y;}
	int getId() {return id;}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeFloat(x);
		out.writeFloat(y);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.x = in.readFloat();
		this.y = in.readFloat();
	}

	public int compareTo(Point rect2) {
		// Sort by id
		double difference = this.x - rect2.x;
		if (difference < 0) {
			return -1;
		} 
		if (difference > 0) {
			return 1;
		}
		return 0;

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
}
