package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.spatial.TigerShape;

public class GridRecordWriter implements RecordWriter<CellInfo, TigerShape> {
  private static final int BufferLength = 1024 * 1024;
  private static long BlockSize = 64 * 1024 * 1024;
  private final GridInfo gridInfo;
  /**An output stream for each grid cell*/
  private static OutputStream[][] cellStreams;
  private static long[][] cellSizes;
  private final Path outFile;
  private final FileSystem fileSystem;
  private Text text;

  public GridRecordWriter(FileSystem fileSystem, Path outFile, GridInfo gridInfo) {
    this.fileSystem = fileSystem;
    this.outFile = outFile;
    this.gridInfo = gridInfo;

    // Prepare arrays that hold streams
    int gridColumns = (int) Math.ceil((double)gridInfo.gridWidth / gridInfo.cellWidth); 
    int gridRows = (int) Math.ceil((double)gridInfo.gridHeight / gridInfo.cellHeight);
    cellStreams = new PrintStream[gridColumns][gridRows];
    cellSizes = new long[gridColumns][gridRows];
    
    text = new Text();
  }

  public synchronized void write(LongWritable dummyId, TigerShape rect) throws IOException {
    rect.toText(text);
    // Write to all possible grid cells
    for (long x = rect.getMBR().getX1(); x < rect.getMBR().getX2(); x += gridInfo.cellWidth) {
      for (long y = rect.getMBR().getY1(); y < rect.getMBR().getY2(); y += gridInfo.cellHeight) {
        OutputStream os = getOrCreateCellFile(x, y);
        write(os, text);
        // increase number of bytes written to this print stream
        int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
        int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
        cellSizes[column][row] += text.getLength() + 1;
      }
    }
  }

  public static void write(OutputStream os, Text text) throws IOException {
    os.write(text.getBytes(), 0, text.getLength());
  }
  
  public synchronized void write(CellInfo cellInfo, TigerShape rect) throws IOException {
    rect.toText(text);
    // Write to the cell given
    OutputStream os = getOrCreateCellFile(cellInfo.x, cellInfo.y);
    write(os, text);
    int column = (int)((cellInfo.x - gridInfo.xOrigin) / gridInfo.cellWidth);
    int row = (int)((cellInfo.y - gridInfo.yOrigin) / gridInfo.cellHeight);
    cellSizes[column][row] += text.getLength() + 1;
  }

  public synchronized void close(Reporter reporter) throws IOException {
    // Create a buffer filled with new lines
    byte[] buffer = new byte[BufferLength];
    for (int i = 0; i < buffer.length; i++)
      buffer[i] = '\n';

    Vector<Path> pathsToConcat = new Vector<Path>();
    // Close all output files
    for (int i = 0; i < cellStreams.length; i++) {
      for (int j = 0; j < cellStreams[i].length; j++) {
        if (cellStreams[i][j] != null) {
          // Stuff all open streams with empty lines until each one is 64 MB
          long remainingBytes = BlockSize - cellSizes[i][j] % BlockSize;
          // Write some bytes so that remainingBytes is multiple of buffer.length
          cellStreams[i][j].write(buffer, 0, (int)(remainingBytes % buffer.length));
          remainingBytes -= remainingBytes % buffer.length;
          // Write chunks of size buffer.length
          while (remainingBytes > 0) {
            cellStreams[i][j].write(buffer);
            remainingBytes -= buffer.length;
          }
          // Close stream
          cellStreams[i][j].close();
          // Append to the list of paths to concatenate
          pathsToConcat.add(getCellFilePath(i, j));
        }
      }
    }

    if (!pathsToConcat.isEmpty()) {
      // Concatenate all files into the first file
      Path firstFile = pathsToConcat.remove(0);

      if (!pathsToConcat.isEmpty()) {
        Path[] paths = pathsToConcat.toArray(new Path[pathsToConcat.size()]);
        ((DistributedFileSystem)fileSystem).concat(firstFile, paths);
      }
      // Rename file to original required filename
      fileSystem.rename(firstFile, outFile);
    }
    
  }

  /**
   * Return path to a file that is used to write one grid cell
   * @param column
   * @param row
   * @return
   */
  private Path getCellFilePath(int column, int row) {
    return new Path(outFile.toUri().getPath() + '_' + column + '_' + row);
  }

  /**
   * Find or create a print stream used to write the point given by x, y.
   * x, y can be any an arbitrary point in the space.
   * The returned print stream is associated with the grid cell that contains
   * the given point.
   * @param x
   * @param y
   * @return
   * @throws IOException
   */
  public OutputStream getOrCreateCellFile(long x, long y) throws IOException {
    int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
    int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
    OutputStream os = cellStreams[column][row];
    if (os == null) {
      os = fileSystem.create(getCellFilePath(column, row), gridInfo);
      cellStreams[column][row] = os;
      long xCell = column * gridInfo.cellWidth + gridInfo.xOrigin;
      long yCell = row * gridInfo.cellHeight + gridInfo.yOrigin;
      System.out.println("Setting next block at "+xCell+","+ yCell);
      ((DFSOutputStream)((FSDataOutputStream)os).getWrappedStream()).setNextBlockCell(xCell, yCell);
    }
    return os;
  }
}
