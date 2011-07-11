package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;

public class TigerPointRecordReader extends TigerShapeRecordReader {

  public TigerPointRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
  }

  @Override
  protected Point createShape() {
    return new Point();
  }

  @Override
  protected void parseShape(Shape shape, String[] parts) {
    Point p = (Point) shape;
    p.x = Long.parseLong(parts[1]);
    p.y = Long.parseLong(parts[2]);
  }

}
