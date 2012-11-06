package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;

/**
 * A record reader for objects of class {@link Shape}
 * @author eldawy
 *
 */
public class ShapeRecordReader<S extends Shape> 
    implements RecordReader<LongWritable, S>{

  /**A stock shape to use for deserialization (value)*/
  private S stockShape;
  
  /**An underlying reader to read lines of the text file*/
  private RecordReader<LongWritable, Text> lineRecordReader;
  
  /**The value used with lineRecordReader*/
  private Text subValue;

  public ShapeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    lineRecordReader = new LineRecordReader(job, split);
    subValue = lineRecordReader.createValue();
    stockShape = createStockShape(job);
  }

  public ShapeRecordReader(InputStream in, long offset, long endOffset) {
    lineRecordReader = new LineRecordReader(in, offset, endOffset, 8192);
    subValue = lineRecordReader.createValue();
  }

  @Override
  public boolean next(LongWritable key, S shape) throws IOException {
    if (!lineRecordReader.next(key, subValue) || subValue.getLength() == 0) {
      // Stop on wrapped reader EOF or an empty line which indicates EOF too
      return false;
    }
    // Convert to a regular string to be able to use split
    shape.fromText(subValue);

    return true;
  }

  @Override
  public void close() throws IOException {
    lineRecordReader.close();
  }
  
  @Override
  public long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override
  public LongWritable createKey() {
    return lineRecordReader.createKey();
  }

  @Override
  public S createValue() {
    return stockShape;
  }

  private S createStockShape(Configuration job) {
    S stockShape = null;
    String shapeClassName =
        job.get(SpatialSite.SHAPE_CLASS, Point.class.getName());
    try {
      Class<? extends Shape> shapeClass =
          Class.forName(shapeClassName).asSubclass(Shape.class);
      stockShape = (S) shapeClass.newInstance();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return stockShape;
  }
}
