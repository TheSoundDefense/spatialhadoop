package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * Information about a specific cell in a grid.
 * Note: Whenever you change the instance variables that need to
 * be stored in disk, you have to manually fix the implementation of class
 * BlockListAsLongs
 * @author aseldawy
 *
 */
public class CellInfo extends Rectangle implements WritableComparable<CellInfo> {
  
  /**
   * A unique ID for this cell in a file. This must be set initially when
   * cells for a file are created. It cannot be guessed from cell dimensions.
   */
  public long cellId;

  /**
   * Loads a cell serialized to the given stream
   * @param in
   * @throws IOException 
   */
  public CellInfo(DataInput in) throws IOException {
    this.readFields(in);
  }

  public CellInfo(String in) {
    this.fromText(new Text(in));
  }

  public CellInfo() {
    super();
  }

  public CellInfo(long id, long x, long y, long width, long height) {
    super(x, y, width, height);
    this.cellId = id;
  }

  public CellInfo(long id, Rectangle cellInfo) {
    this(id, cellInfo.x, cellInfo.y, cellInfo.width, cellInfo.height);
  }
  
  @Override
  public String toString() {
    return "Cell #"+cellId+" "+super.toString();
  }
  
  @Override
  public CellInfo clone() {
    return new CellInfo(cellId, x, y, width, height);
  }
  
  @Override
  public boolean equals(Object obj) {
    return ((CellInfo)obj).cellId == this.cellId;
  }
  
  @Override
  public int hashCode() {
    return (int) this.cellId;
  }
  
  @Override
  public int compareTo(Shape s) {
    return (int) (this.cellId - ((CellInfo)s).cellId);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(cellId);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.cellId = in.readLong();
    super.readFields(in);
  }

  @Override
  public int compareTo(CellInfo c) {
    return (int) (this.cellId - c.cellId);
  }
}
