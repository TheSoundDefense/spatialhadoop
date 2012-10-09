package org.apache.hadoop.spatial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RTreeGridRecordWriter implements TigerShapeRecordWriter {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  
  /**The default RTree degree used for local indexing*/
  public static final String RTREE_DEGREE = "spatialHadoop.storage.RTreeDegree";
  
  public static final long RTreeFileMarker = -0x0123456;

  /**An output stream for each grid cell*/
  private CellInfo[] cells;
  private Vector<TigerShape>[] cellShapes;
  private FSDataOutputStream[] cellStreams;
  private final Path outFile;
  private final FileSystem fileSystem;
  private final int rtreeDegree;
  private final long rtreeLimit;
  
  @SuppressWarnings("unchecked")
  public RTreeGridRecordWriter(FileSystem fileSystem, Path outFile, CellInfo[] cells) throws IOException {
    LOG.info("Writing to RTrees");
    this.fileSystem = fileSystem;
    this.outFile = outFile;
    this.cells = cells;
    this.rtreeDegree = fileSystem.getConf().getInt(RTREE_DEGREE, 5);
    this.rtreeLimit = RTree.getBlockCapacity(fileSystem.getDefaultBlockSize()-8,
        rtreeDegree, calculateRecordSize(TigerShape.class));

    // Prepare arrays that hold records before building the RTree
    cellShapes = new Vector[cells.length];
    cellStreams = new FSDataOutputStream[cells.length];

    for (CellInfo cell : cells ) {
      LOG.info("Partitioning according to cell: " + cell);
    }

    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      cellShapes[cellIndex] = new Vector<TigerShape>();
    }
  }
  
  /**
   * Calculates number of bytes needed to serialize an instance of the given
   * class. It writes a dummy object to a ByteArrayOutputStreams and calculates
   * number of bytes reserved by this stream.
   * @param klass
   * @return
   */
  public static int calculateRecordSize(Class<? extends Writable> klass) {
    int size = -1;
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bout);
      klass.newInstance().write(out);
      out.close();
      size = bout.toByteArray().length;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return size;
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
    if (shape == null) {
      closeCell(cells[cellIndex]);
      return;
    }
    cellShapes[cellIndex].add((TigerShape) shape.clone());
    if (cellShapes[cellIndex].size() >= rtreeLimit) {
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
      if (cellShapes[cellIndex] != null && !cellShapes[cellIndex].isEmpty()) {
        flushCell(cellIndex);
        finalizeCell(cellIndex);
      }
      
      Path cellPath = getCellFilePath(cellIndex);
      if (fileSystem.exists(cellPath))
        pathsToConcat.add(cellPath);
    }

    LOG.info("Closing... Merging "+pathsToConcat.size());
    if (pathsToConcat.size() == 1) {
      fileSystem.rename(pathsToConcat.firstElement(), outFile);
    } else {
      if (!pathsToConcat.isEmpty()) {
        // Concat requires the target file to be a non-empty file with the same
        // block size as source files
        Path target = pathsToConcat.lastElement();
        pathsToConcat.remove(pathsToConcat.size()-1);
        Path[] paths = pathsToConcat.toArray(new Path[pathsToConcat.size()]);
        fileSystem.concat(target, paths);
        fileSystem.rename(target, outFile);
      }
      LOG.info("Concatenated files into: "+outFile);
    }
    LOG.info("Final file size: "+fileSystem.getFileStatus(outFile).getLen());
  }
  
  private void flushCell(int cellIndex) throws IOException {
    LOG.info(this+"Writing the RTree at cell #"+cellIndex);

    if (cellShapes[cellIndex].size() > 0) {
      FSDataOutputStream cellStream = getCellStream(cellIndex);
      cellStream.writeLong(RTreeFileMarker);
      RTree<TigerShape> rtree = new RTree<TigerShape>();
      TigerShape[] shapes = new TigerShape[cellShapes[cellIndex].size()];
      cellShapes[cellIndex].toArray(shapes);
      cellShapes[cellIndex].clear();
      rtree.bulkLoad(shapes, rtreeDegree);
      shapes = null;
      LOG.info("Writing RTree to disk");
      rtree.write(cellStream);

      // Stuff the file with bytes to make a complete block
      long blockSize = fileSystem.getDefaultBlockSize();
      long cellSize = cellStream.getPos();
      LOG.info("Current size: "+cellSize);
      // Stuff all open streams with empty lines until each one is 64 MB
      long remainingBytes = (blockSize - cellSize % blockSize) % blockSize;
      LOG.info("Stuffing file " + cellIndex +  " with new lines: " + remainingBytes);
      // Create a buffer filled with zeros
      byte[] buffer = new byte[fileSystem.getConf().getInt("io.file.buffer.size", 1024 * 1024)];
      Arrays.fill(buffer, (byte)0);
      // Write some bytes so that remainingBytes is multiple of buffer.length
      cellStream.write(buffer, 0, (int)(remainingBytes % buffer.length));
      remainingBytes -= remainingBytes % buffer.length;
      // Write chunks of size buffer.length
      while (remainingBytes > 0) {
        cellStream.write(buffer);
        remainingBytes -= buffer.length;
      }
      buffer = null;

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
    if (cellStreams[cellIndex] == null) {
      Path cellFilePath = getCellFilePath(cellIndex);
      if (!fileSystem.exists(cellFilePath)) {
        // Create new file
        cellStreams[cellIndex] = fileSystem.create(cellFilePath, cells[cellIndex]);
      } else {
        // Append to existing file
        cellStreams[cellIndex] = fileSystem.append(cellFilePath);
      }
    }
    return cellStreams[cellIndex];
  }

  @Override
  public void closeCell(CellInfo cellInfo) throws IOException {
    int cellIndex = locateCell(cellInfo);
    flushCell(cellIndex);
    finalizeCell(cellIndex);
  }
}
