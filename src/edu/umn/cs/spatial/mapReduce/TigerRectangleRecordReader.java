package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;

public class TigerRectangleRecordReader extends TigerShapeRecordReader {

  public TigerRectangleRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
  }

  @Override
  protected Shape createShape() {
    return new Rectangle();
  }

  @Override
  protected void parseShape(Shape shape, String[] parts) {
    Rectangle r = (Rectangle) shape;
    r.x = Long.parseLong(parts[1]);
    r.y = Long.parseLong(parts[2]);
    r.width = Long.parseLong(parts[3]);
    r.height = Long.parseLong(parts[4]);
  }

}
