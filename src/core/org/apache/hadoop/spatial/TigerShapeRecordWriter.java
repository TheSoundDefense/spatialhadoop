package org.apache.hadoop.spatial;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public interface TigerShapeRecordWriter extends Closeable {
  /**
   * Writes the given shape to the file to all cells it overlaps with
   * @param dummyId
   * @param shape
   * @throws IOException
   */
  public void write(LongWritable dummyId, TigerShape shape) throws IOException;
  
  /**
   * Writes the given shape to the file to all cells it overlaps with.
   * Text is passed to avoid serializing the shape if it is already serialized.
   * @param dummyId
   * @param shape
   * @param text
   * @throws IOException
   */
  public void write(LongWritable dummyId, TigerShape shape, Text text) throws IOException;
  
  /**
   * Writes the given shape only to the given cell even if it overlaps
   * with other cells.
   * @param cellInfo
   * @param shape
   * @throws IOException
   */
  public void write(CellInfo cellInfo, TigerShape shape) throws IOException;

  /**
   * Writes the given shape only to the given cell even if it overlaps.
   * The passed text is used to write to file to avoid serializing the shape.
   * @param cellInfo
   * @param shape
   * @param text
   * @throws IOException
   */
  public void write(CellInfo cellInfo, TigerShape shape, Text text) throws IOException;
  
  /**
   * Writes the given text, whatever it represents, to the given cell.
   * Usually this text represent one or more shapes.
   * @param cellInfo
   * @param text
   * @throws IOException
   */
  public void write(CellInfo cellInfo, Text text) throws IOException;
  
  /**
   * Write an array of bytes to a certain cell.
   * This is used with RTree blocks when we need to write an RTree representing
   * a bulk of entries.
   * @param cellInfo
   * @param buffer
   * @throws IOException
   */
  public void write(CellInfo cellInfo, BytesWritable buffer) throws IOException;
}
