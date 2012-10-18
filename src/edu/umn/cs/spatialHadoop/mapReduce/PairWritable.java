package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**A class that stores a pair of objects where both are writable*/
public class PairWritable<T extends Writable> extends Pair<T>
    implements Writable {
  
  public PairWritable() {}
  
  public PairWritable(T first, T second) {
    super(first, second);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

}