package edu.umn.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Writable;


/**
 * An implementation of ArrayList that is Writable.
 * TODO change this to a wrapper class of any Collection 
 * @author aseldawy
 *
 * @param <E>
 */
public class ArrayListWritable<E extends Writable> extends ArrayList<E>
		implements CollectionWritable<E> {

	/**
	 * Auto generated
	 */
	private static final long serialVersionUID = 38833809946948856L;

	public ArrayListWritable() {
	}

	public ArrayListWritable(int initialCapacity) {
		super(initialCapacity);
	}

	public ArrayListWritable(Collection<E> c) {
		super(c);
	}

	public void write(DataOutput out) throws IOException {
		// Write size
		out.writeInt(this.size());
		for (Writable obj : this) {
			// Write class of this class
			out.writeUTF(obj.getClass().getName());
			// Write object itself
			obj.write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		// Clear all existing values
		super.clear();
		// Read size
		int size = in.readInt();
		while (size-- > 0) {
			Writable obj = null;
			// Read class name
			String className = in.readUTF();
			
			// Initialize an empty object of this type
			// And read its data from the stream
			try {
				Class klass = Class.forName(className);
				obj = (Writable) klass.newInstance();
				obj.readFields(in);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			// Add the in
			super.add((E)obj);
		}
	}

}
