package spatial;

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
public class Rectangle implements WritableComparable<Rectangle>, Serializable {
	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = 7801822896513739736L;
	
	public int id;
	public float x1;
	public float x2;
	public float y1;
	public float y2;

	public Rectangle() {
		this.set(0, 0.0f, 0.0f, 0.0f, 0.0f);
	}
	
	public Rectangle(int id, float x1, float y1, float x2, float y2) {
		this.set(id, x1, y1, x2, y2);
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeFloat(x1);
		out.writeFloat(y1);
		out.writeFloat(x2);
		out.writeFloat(y2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.x1 = in.readFloat();
		this.y1 = in.readFloat();
		this.x2 = in.readFloat();
		this.y2 = in.readFloat();
	}

	@Override
	public int compareTo(Rectangle rect2) {
		// Sort by id
		return this.id - rect2.id;
	}
	
	@Override
	public String toString() {
		return "Rectangle #"+id+" ("+x1+","+y1+","+x2+","+y2+")";
	}
}
