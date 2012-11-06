package edu.umn.cs.spatialHadoop.mapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.spatial.Shape;

/**A class that stores a pair of object for spatial join*/
public class PairShape<T extends Shape> extends PairWritable<T>
    implements TextSerializable {
  public static final byte[] SeparatorBytes = Separator.getBytes();

  public PairShape() {
    super();
  }

  public PairShape(T first, T second) {
    super(first, second);
  }

  @Override
  public Text toText(Text text) {
    first.toText(text);
    text.append(SeparatorBytes, 0, SeparatorBytes.length);
    return second.toText(text);
  }

  @Override
  public void fromText(Text text) {
    String[] parts = text.toString().split(Separator, 2);
    first.fromText(new Text(parts[0]));
    second.fromText(new Text(parts[1]));
  }
}