package spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class PairOfRectangles implements WritableComparable<PairOfRectangles>, Serializable {
	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = -257992220312398963L;

	public final Rectangle r1;
	public final Rectangle r2;

	public PairOfRectangles() {
		r1 = new Rectangle();
		r2 = new Rectangle();
	}
	
	public PairOfRectangles(Rectangle r1, Rectangle r2) {
		this.r1 = r1;
		this.r2 = r2;
	}
	
	@Override
	public boolean equals(Object obj) {
		PairOfRectangles x = (PairOfRectangles) obj;
		return this.r1.equals(x.r1) && this.r2.equals(x.r2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		r1.write(out);
		r2.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		r1.readFields(in);
		r2.readFields(in);
	}

	@Override
	public int compareTo(PairOfRectangles o) {
		int c = this.r1.compareTo(o.r1); 
		return c == 0 ? this.r2.compareTo(o.r2) : c;
	}

	@Override
	public String toString() {
		return "<"+r1+">,<"+r2+">";
	}
}
