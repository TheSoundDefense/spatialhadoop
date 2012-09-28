package org.apache.hadoop.spatial;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class RTreeGridRecordWriter implements TigerShapeRecordWriter {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  public static final long RTreeFileMarker = -0x0123456;
  public static final int RTREE_MAX_ENTRIES = 200;

  /**Maximum number of entries per RTree. Affects memory usage.*/
  private static final int RTREE_LIMIT = 600000;

  /**An output stream for each grid cell*/
  private CellInfo[] cells;
  private List<TigerShape>[] cellShapes;
  private final Path outFile;
  private final FileSystem fileSystem;
  
  public RTreeGridRecordWriter(FileSystem fileSystem, Path outFile, CellInfo[] cells) throws IOException {
    LOG.info("Writing to RTrees");
    this.fileSystem = fileSystem;
    this.outFile = outFile;
    this.cells = cells;

    // Prepare arrays that hold streams
    cellShapes = new List[cells.length];

    for (CellInfo cell : cells ) {
      LOG.info("Partitioning according to cell: " + cell);
    }

    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      cellShapes[cellIndex] = new Vector<TigerShape>();
    }
  }

  /**
   * Writes the given shape to the current grid file. The text representation that need
   * to be written is passed for two reasons.
   * 1 - Avoid converting the shape to text. (Performance)
   * 2 - Some information may have been lost from the original line when converted to shape
   * The text is assumed not to have a new line. This method writes a new line to the output
   * after the given text is written.
   * @param dummyId
   * @param shape
   * @param text
   * @throws IOException
   */
  public synchronized void write(LongWritable dummyId, TigerShape shape) throws IOException {
    // Write to all possible grid cells
    Rectangle mbr = shape.getMBR();
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      if (mbr.isIntersected(cells[cellIndex])) {
        writeToCell(cellIndex, shape);
      }
    }
  }
  
  public synchronized void write(LongWritable dummyId, TigerShape shape, Text text) throws IOException {
    write(dummyId, shape);
  }
  
  /**
   * Write the given shape to a specific cell. The shape is not replicated to any other cells.
   * It's just written to the given cell. This is useful when shapes are already assigned
   * and replicated to grid cells another way, e.g. from a map phase that partitions.
   * @param cellInfo
   * @param shape
   * @throws IOException
   */
  public synchronized void write(CellInfo cellInfo, TigerShape shape) throws IOException {
    // Write to the given cell
    writeToCell(locateCell(cellInfo), shape);
  }

  public synchronized void write(CellInfo cellInfo, TigerShape shape, Text text) throws IOException {
    write(cellInfo, shape);
  }
  
  /**
   * A low level method to write a text to a specified cell.
   * 
   * @param cellIndex
   * @param text
   * @throws IOException
   */
  private synchronized void writeToCell(int cellIndex, TigerShape shape) throws IOException {
    cellShapes[cellIndex].add((TigerShape) shape.clone());

    if (cellShapes[cellIndex].size() >= RTREE_LIMIT) {
      flushCell(cellIndex);
    }
  }
  
  public synchronized void write(CellInfo cellInfo, Text text) throws IOException {
    throw new RuntimeException("Not supported");
  }
  
  public synchronized void write(CellInfo cellInfo, BytesWritable bytes) throws IOException {
    int cellIndex = locateCell(cellInfo);
    FSDataOutputStream dos = getCellStream(cellIndex);
    dos.writeLong(RTreeFileMarker);
    dos.write(bytes.getBytes());
    dos.close();
  }
  
  private int locateCell(CellInfo cellInfo) {
    // TODO use a hash table for faster locating of cells
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++)
      if (cells[cellIndex].equals(cellInfo))
        return cellIndex;
    LOG.error("Could not find: "+cellInfo);
    LOG.error("In this list");
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      LOG.error("#"+cellIndex+": " + cells[cellIndex]);
    }
    throw new RuntimeException("Could not locate required cell");
  }

  public synchronized void close() throws IOException {
    Vector<Path> pathsToConcat = new Vector<Path>();
   
    // Write all RTrees to files
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      flushCell(cellIndex);
      finalizeCell(cellIndex);
      
      Path cellPath = getCellFilePath(cellIndex);
      if (fileSystem.exists(cellPath))
        pathsToConcat.add(cellPath);
    }

    if (!pathsToConcat.isEmpty()) {
      // Concatenate all files into the first file
      Path firstFile = pathsToConcat.remove(0);

      if (!pathsToConcat.isEmpty()) {
        Path[] paths = pathsToConcat.toArray(new Path[pathsToConcat.size()]);
        fileSystem.concat(firstFile, paths);
      }
      // Rename file to original required filename
      fileSystem.rename(firstFile, outFile);
    }
  }
  
  private void flushCell(int cellIndex) throws IOException {
    LOG.info(this+"Writing the RTree at cell #"+cellIndex);
    // Create a buffer filled with zeros
    byte[] buffer = new byte[fileSystem.getConf().getInt("io.file.buffer.size", 1024 * 1024)];
    Arrays.fill(buffer, (byte)0);

    if (cellShapes[cellIndex].size() > 0) {
      FSDataOutputStream dos = getCellStream(cellIndex);
      dos.writeLong(RTreeFileMarker);
      RTree<TigerShape> rtree = new RTree<TigerShape>();
      rtree.bulkLoad(cellShapes[cellIndex].
          toArray(new TigerShape[cellShapes[cellIndex].size()]),
          RTREE_MAX_ENTRIES);
      LOG.info("Writing RTree to disk");
      rtree.write(dos);
      cellShapes[cellIndex].clear();
      dos.close();
      
      LOG.info("Actual size: "+fileSystem.getFileStatus(getCellFilePath(cellIndex)).getLen());
    }
  }

  private void finalizeCell(int cellIndex) throws IOException {
    // Create a buffer filled with new lines
    byte[] buffer = new byte[fileSystem.getConf().getInt("io.file.buffer.size", 1024 * 1024)];
    for (int i = 0; i < buffer.length; i++)
      buffer[i] = '\0';
    
    long blockSize = fileSystem.getDefaultBlockSize();

    if (fileSystem.exists(getCellFilePath(cellIndex))) {
      long cellSize = fileSystem.getFileStatus(getCellFilePath(cellIndex)).getLen();
      OutputStream cellStream = getCellStream(cellIndex);
      LOG.info("Current size: "+cellSize);
      // Stuff all open streams with empty lines until each one is 64 MB
      long remainingBytes = (blockSize - cellSize % blockSize) % blockSize;
      LOG.info("Stuffing file " + cellIndex +  " with new lines: " + remainingBytes);
      // Write some bytes so that remainingBytes is multiple of buffer.length
      cellStream.write(buffer, 0, (int)(remainingBytes % buffer.length));
      remainingBytes -= remainingBytes % buffer.length;
      // Write chunks of size buffer.length
      while (remainingBytes > 0) {
        cellStream.write(buffer);
        remainingBytes -= buffer.length;
      }
      // Close stream
      cellStream.close();
      LOG.info("Actual size: "+fileSystem.getFileStatus(getCellFilePath(cellIndex)).getLen());
    }
  }
  
  /**
   * Return path to a file that is used to write one grid cell
   * @param column
   * @param row
   * @return
   */
  private Path getCellFilePath(int cellIndex) {
    return new Path(outFile.toUri().getPath() + '_' + cellIndex);
  }
  
  private FSDataOutputStream getCellStream(int cellIndex) throws IOException {
    Path cellFilePath = getCellFilePath(cellIndex);
    FSDataOutputStream cellStream;
    if (!fileSystem.exists(cellFilePath)) {
      // Create new file
      cellStream = fileSystem.create(cellFilePath, cells[cellIndex]);
    } else {
      // Append to existing file
      cellStream = fileSystem.append(cellFilePath);
    }
    return cellStream;
  }

  @Override
  public void closeCell(CellInfo cellInfo) throws IOException {
    throw new RuntimeException("Not implemented RTreeGridRecordWriter#closeCell");
  }
}
