package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * A very simple spatial index that provides some spatial operations based
 * on an array storage.
 * @author eldawy
 *
 * @param <S>
 */
public class SimpleSpatialIndex<S extends Shape> implements Writable,
    Iterable<S> {
  /**A stock instance of S used to deserialize objects from disk*/
  protected S stockShape;
  
  /**All underlying shapes in no specific order*/
  protected S[] shapes;
  
  public SimpleSpatialIndex() {
  }
  
  @SuppressWarnings("unchecked")
  public void bulkLoad(S[] shapes) {
    // Create a shallow copy
    this.shapes = shapes.clone();
    // Change it into a deep copy by cloning each instance
    for (int i = 0; i < this.shapes.length; i++) {
      this.shapes[i] = (S) this.shapes[i].clone();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(shapes.length);
    for (int i = 0; i < shapes.length; i++) {
      shapes[i].write(out);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    this.shapes = (S[]) new Shape[length];
    for (int i = 0; i < length; i++) {
      this.shapes[i] = (S) stockShape.clone();
      this.shapes[i].readFields(in);
    }
  }
  
  public int rangeQuery(Shape queryRange, ResultCollector<S> output) {
    int result_count = 0;
    for (S shape : shapes) {
      if (shape.isIntersected(queryRange)) {
        result_count++;
        if (output != null) {
          output.collect(shape);
        }
      }
    }
    return result_count;
  }
  
  public static<S1 extends Shape, S2 extends Shape>
      int spatialJoin(SimpleSpatialIndex<S1> s1, SimpleSpatialIndex<S2> s2,
          final ResultCollector2<S1, S2> output) {
    return SpatialAlgorithms.SpatialJoin_planeSweep(s1.shapes, s2.shapes, output);
  }
  
  /**
   * A simple iterator over all shapes in this index
   * @author eldawy
   *
   */
  class SimpleIterator implements Iterator<S> {
    
    /**Current index*/
    int i = 0;

    @Override
    public boolean hasNext() {
      return i < shapes.length;
    }

    @Override
    public S next() {
      return shapes[i++];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not implemented");
    }
    
  }

  @Override
  public Iterator<S> iterator() {
    return new SimpleIterator();
  }
  
  /**
   * Number of objects stored in the index
   * @return
   */
  public int size() {
    return shapes.length;
  }
}
