package org.apache.hadoop.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Writable;

/**
 * Stores grid information that can be used with spatial files.
 * @author aseldawy
 *
 */
public class GridInfo implements Writable, TextSerializable {
  public long xOrigin, yOrigin;
  public long gridWidth, gridHeight;
  public long cellWidth, cellHeight;

  public GridInfo() {
  }
  
  public GridInfo(long xOrigin, long yOrigin, long gridWidth,
      long gridHeight, long cellWidth, long cellHeight) {
    this.xOrigin = xOrigin;
    this.yOrigin = yOrigin;
    this.gridWidth = gridWidth;
    this.gridHeight = gridHeight;
    this.cellWidth = cellWidth;
    this.cellHeight = cellHeight;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(xOrigin);
    out.writeLong(yOrigin);
    out.writeLong(gridWidth);
    out.writeLong(gridHeight);
    out.writeLong(cellWidth);
    out.writeLong(cellHeight);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    xOrigin = in.readLong();
    yOrigin = in.readLong();
    gridWidth = in.readLong();
    gridHeight = in.readLong();
    cellWidth = in.readLong();
    cellHeight = in.readLong();
  }

  @Override
  public String toString() {
    return "grid: "+xOrigin+","+yOrigin+","+(xOrigin+gridWidth)+","+
    (yOrigin+gridHeight)+", cell: "+cellWidth+","+cellHeight+"("+getGridColumns()+"x"+getGridRows()+")";
  }
  
  @Override
  public boolean equals(Object obj) {
    GridInfo gi = (GridInfo) obj;
    return this.xOrigin == gi.xOrigin && this.yOrigin == gi.yOrigin
        && this.gridWidth == gi.gridWidth && this.gridHeight == gi.gridHeight
        && this.cellWidth == gi.cellWidth && this.cellHeight == gi.cellHeight;
  }
  
  /**
   * Get CellInfo for the cell that contains the given point
   * @param x
   * @param y
   * @return
   */
/*  public CellInfo getCellInfo(long x, long y) {
    x -= xOrigin;
    x -= x % cellWidth;
    x += xOrigin;
    
    y -= yOrigin;
    y -= y % cellHeight;
    y += yOrigin;
    
    return new CellInfo(x, y, cellWidth, cellHeight);
  }
  */
  public String writeToString() {
    return String.format("%s%x,%s%x,%s%x,%s%x,%s%x,%s%x", xOrigin < 0 ? "-" : "", Math.abs(xOrigin),
        yOrigin < 0 ? "-" : "", Math.abs(yOrigin),
            gridWidth < 0 ? "-" : "", Math.abs(gridWidth),
                gridHeight < 0 ? "-" : "", Math.abs(gridHeight),
                    cellWidth < 0 ? "-" : "", Math.abs(cellWidth),
                        cellHeight < 0 ? "-" : "", Math.abs(cellHeight));
  }
  
  public void readFromString(String string) {
    String[] parts = string.split(",");
    this.xOrigin = Long.parseLong(parts[0], 16);
    this.yOrigin = Long.parseLong(parts[1], 16);
    this.gridWidth = Long.parseLong(parts[2], 16);
    this.gridHeight = Long.parseLong(parts[3], 16);
    if (parts.length > 4) {
      this.cellWidth = Long.parseLong(parts[4], 16);
      this.cellHeight = Long.parseLong(parts[5], 16);
    }
  }
  
  public int getGridColumns() {
    return (int) Math.ceil((double)gridWidth / cellWidth);
  }
  
  public int getGridRows() {
    return (int) Math.ceil((double)gridHeight / cellHeight);
  }
  
  public void calculateCellDimensions(long totalFileSize, long blockSize) {
    // An empirical number for the expected overhead in grid file due to
    // replication
    final double GridOverhead = 0.001;
    totalFileSize += totalFileSize * GridOverhead;
    int numBlocks = (int) Math.ceil((double)totalFileSize / blockSize);
    calculateCellDimensions(numBlocks);
  }
  
  public void calculateCellDimensions(int numCells) {
    int gridCols = 1;
    int gridRows = 1;
    while (gridRows * gridCols < numCells) {
      // (  cellWidth          >    cellHeight        )
      if (gridWidth / gridCols > gridHeight / gridRows) {
        gridCols++;
      } else {
        gridRows++;
      }
    }
    cellWidth = (long)Math.ceil((double)gridWidth / gridCols);
    cellHeight = (long)Math.ceil((double)gridHeight / gridRows);
  }

  @Override
  public void toText(Text text) {
    String str = writeToString();
    text.append(str.getBytes(), 0, str.length());
  }

  @Override
  public void fromText(Text text) {
    readFromString(text.toString());
  }

  public Rectangle getMBR() {
    return new Rectangle(xOrigin, yOrigin, gridWidth, gridHeight);
  }

  public CellInfo[] getAllCells() {
    int cellIndex = 0;
    CellInfo[] cells = new CellInfo[getGridColumns() * getGridRows()];
    for (int col = 0; col < getGridColumns(); col++) {
      for (int row = 0; row < getGridRows(); row++) {
        cells[cellIndex] = new CellInfo(cellIndex, xOrigin + gridWidth * col / getGridColumns(),
            yOrigin + gridHeight * row / getGridRows(),
            cellWidth, cellHeight);
        cellIndex++;
      }
    }
    return cells;
  }

}
