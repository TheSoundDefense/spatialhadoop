package edu.umn.cs.spatialHadoop.mapReduce;

import org.apache.hadoop.io.WritableComparable;

/**
 * A pair that is both writable and comparable. Used as a key for the output
 * of the map function.
 * @author eldawy
 *
 */
public class PairWritableComparable<T extends WritableComparable>
  extends PairWritable<T> implements WritableComparable<PairWritableComparable<T>>{

  public PairWritableComparable() {
    super();
  }

  public PairWritableComparable(T first, T second) {
    super(first, second);
  }

  @Override
  public int compareTo(PairWritableComparable<T> o) {
    int c = first.compareTo(o.first);
    if (c != 0)
      return c;
    return second.compareTo(o.second);
  }

}
