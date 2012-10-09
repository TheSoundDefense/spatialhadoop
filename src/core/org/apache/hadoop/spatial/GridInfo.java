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
 * The grid is uniform which means all cells have the same width and the same
 * height.
 * @author aseldawy
 *
 */
public class GridInfo implements Writable, TextSerializable {
  public long xOrigin, yOrigin;
  public long gridWidth, gridHeight;
  public int columns, rows;

  public GridInfo() {
  }
  
  public GridInfo(long xOrigin, long yOrigin, long gridWidth,
      long gridHeight, long cellWidth, long cellHeight) {
    this.xOrigin = xOrigin;
    this.yOrigin = yOrigin;
    this.gridWidth = gridWidth;
    this.gridHeight = gridHeight;
    this.columns = (int) Math.ceil((float)gridWidth / cellWidth);
    this.rows = (int) Math.ceil((float)gridHeight / cellHeight);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(xOrigin);
    out.writeLong(yOrigin);
    out.writeLong(gridWidth);
    out.writeLong(gridHeight);
    out.writeInt(columns);
    out.writeInt(rows);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    xOrigin = in.readLong();
    yOrigin = in.readLong();
    gridWidth = in.readLong();
    gridHeight = in.readLong();
    columns = in.readInt();
    rows = in.readInt();
  }

  @Override
  public String toString() {
    return "grid: "+xOrigin+","+yOrigin+","+(xOrigin+gridWidth)+","+
    (yOrigin+gridHeight)+", " +
    "cell: "+getAverageCellWidth()+","+getAverageCellHeight()+
    "("+columns+"x"+rows+")";
  }
  
  public long getAverageCellHeight() {
    return gridHeight / rows;
  }

  public long getAverageCellWidth() {
    return gridWidth / columns;
  }

  @Override
  public boolean equals(Object obj) {
    GridInfo gi = (GridInfo) obj;
    return this.xOrigin == gi.xOrigin && this.yOrigin == gi.yOrigin
        && this.gridWidth == gi.gridWidth && this.gridHeight == gi.gridHeight
        && this.columns == gi.columns && this.rows == gi.rows;
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
                  columns < 0 ? "-" : "", Math.abs(columns),
                    rows < 0 ? "-" : "", Math.abs(rows));
  }
  
  public void readFromString(String string) {
    String[] parts = string.split(",");
    this.xOrigin = Long.parseLong(parts[0], 16);
    this.yOrigin = Long.parseLong(parts[1], 16);
    this.gridWidth = Long.parseLong(parts[2], 16);
    this.gridHeight = Long.parseLong(parts[3], 16);
    if (parts.length > 4) {
      this.columns = Integer.parseInt(parts[4], 16);
      this.rows = Integer.parseInt(parts[5], 16);
    }
  }
  
  public void calculateCellDimensions(long totalFileSize, long blockSize) {
    // An empirical number for the expected overhead in grid file due to
    // replication
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
    columns = gridCols;
    rows = gridRows;
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
    CellInfo[] cells = new CellInfo[columns * rows];
    long x1 = xOrigin;
    for (int col = 0; col < columns; col++) {
      long x2 = xOrigin + gridWidth * (col+1) / columns;
      
      long y1 = yOrigin;
      for (int row = 0; row < rows; row++) {
        long y2 = yOrigin + gridHeight * (row+1) / rows;
        cells[cellIndex] = new CellInfo(cellIndex, x1, y1, x2 - x1, y2 - y1);
        cellIndex++;
        
        y1 = y2;
      }
      x1 = x2;
    }
    return cells;
  }

}
