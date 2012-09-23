package edu.umn.cs.spatialHadoop.mapReduce;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatialHadoop.TigerShapeWithIndex;


/**
 * Parses text lines into instances of TigerShape class.
 * You can specify a subclass of TigerShape to be instantiated and returned for each line
 * by setting the property SHAPE_CLASS to the class name to be created.
 * @author aseldawy
 *
 */
public class TigerShapeRecordReader implements RecordReader<LongWritable, TigerShape> {
  public static final Log LOG = LogFactory.getLog(TigerShapeRecordReader.class);
  public static final String TIGER_SHAPE_CLASS = "edu.umn.cs.spatial.mapReduce.TigerShapeRecordReader.TigerShapeClass";
  private static String TigerShapeClassName;
  public static final String SHAPE_CLASS = "edu.umn.cs.spatial.mapReduce.TigerShapeRecordReader.ShapeClass";
  private static String ShapeClassName;
  private RecordReader<LongWritable, Text> lineRecordReader;
  private Text subValue;
  private int index;

  public TigerShapeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    LOG.info("Start parsing split: "+split);
    Thread.dumpStack();
    lineRecordReader = new LineRecordReader(job, split);
    TigerShapeClassName = job.get(TIGER_SHAPE_CLASS, TigerShape.class.getName());
    ShapeClassName = job.get(SHAPE_CLASS, Point.class.getName());
  }
  
  public TigerShapeRecordReader(Configuration job, InputStream in, long offset,
      long endOffset) {
    lineRecordReader = new LineRecordReader(in, offset, endOffset, 8192);
    TigerShapeClassName = job
        .get(TIGER_SHAPE_CLASS, TigerShape.class.getName());
    ShapeClassName = job.get(SHAPE_CLASS, Point.class.getName());
  }

  /**
   * Appends the given index to all objects read by this reader.
   * Used to identify argument index for binary operators such as spatial join
   * @param job
   * @param split
   * @param index
   * @throws IOException
   */
  public TigerShapeRecordReader(Configuration job, FileSplit split, int index)
  throws IOException {
    this(job, split);
    this.index = index;
  }

  /**
	 * Reads a rectangle and emits it.
	 * It skips bytes until end of line is found.
	 * After this, it starts reading rectangles with a line in each rectangle.
	 * It consumes the end of line also when reading the rectangle.
	 * It stops after reading the first end of line (after) end.
	 */
	public boolean next(LongWritable key, TigerShape value) throws IOException {
	  if (!lineRecordReader.next(key, subValue) || subValue.getLength() < 4) {
	    // Stop on wrapped reader EOF or a very short line which indicates EOF too
	    LOG.info("Finished parsing of split");
	    return false;
	  }
	  // Convert to a regular string to be able to use split
	  value.fromText(subValue);

	  return true;
	}

  public long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  public void close() throws IOException {
    lineRecordReader.close();
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override
  public LongWritable createKey() {
    return lineRecordReader.createKey();
  }

  @Override
  public TigerShape createValue() {
    subValue = lineRecordReader.createValue();
    TigerShape tigerShape;
    try {
      Class<TigerShape> tigerShapeClass = (Class<TigerShape>) Class.forName(TigerShapeClassName);
      tigerShape = tigerShapeClass.newInstance();
      // TODO find a cleaner way to set index
      if (tigerShape instanceof TigerShapeWithIndex) {
        ((TigerShapeWithIndex)tigerShape).index = index;
      }
      // Create a concrete shape class to be serialized
      Class<Shape> shapeClass = (Class<Shape>)Class.forName(ShapeClassName);
      tigerShape.shape = shapeClass.newInstance();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      tigerShape = new TigerShape();
    } catch (InstantiationException e) {
      e.printStackTrace();
      tigerShape = new TigerShape();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      tigerShape = new TigerShape();
    }
    return tigerShape;
  }

}
