package org.apache.hadoop.spatial;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public interface ShapeRecordWriter extends Closeable {
  /**
   * Writes the given shape to the file to all cells it overlaps with
   * @param dummyId
   * @param shape
   * @throws IOException
   */
  public void write(LongWritable dummyId, Shape shape) throws IOException;
  
  /**
   * Writes the given shape to the file to all cells it overlaps with.
   * Text is passed to avoid serializing the shape if it is already serialized.
   * @param dummyId
   * @param shape
   * @param text
   * @throws IOException
   */
  public void write(LongWritable dummyId, Shape shape, Text text) throws IOException;
  
  /**
   * Writes the given shape only to the given cell even if it overlaps
   * with other cells. This is used when the output is prepared to write
   * only one cell. The caller ensures that another call will write the object
   * to the other cell(s) later.
   * @param cellInfo
   * @param shape
   * @throws IOException
   */
  public void write(CellInfo cellInfo, Shape shape) throws IOException;

  /**
   * Writes the given shape only to the given cell even if it overlaps.
   * The passed text is used to write to file to avoid serializing the shape.
   * @param cellInfo
   * @param shape
   * @param text
   * @throws IOException
   */
  public void write(CellInfo cellInfo, Shape shape, Text text) throws IOException;
}
