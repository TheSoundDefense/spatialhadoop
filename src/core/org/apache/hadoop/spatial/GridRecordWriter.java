package org.apache.hadoop.spatial;

import java.io.ByteArrayOutputStream;
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

public class GridRecordWriter implements TigerShapeRecordWriter {
  public static final Log LOG = LogFactory.getLog(GridRecordWriter.class);
  /**An output stream for each grid cell*/
  private CellInfo[] cells;
  private OutputStream[] cellStreams;
  private final Path outFile;
  private final FileSystem fileSystem;
  private Text text;
  private final GridInfo gridInfo;

  /**
   * New line marker
   */
  private static final byte[] NEW_LINE = {'\n'};
  
  /**
   * Store all information associated with a cell file
   * @author eldawy
   *
   */
  class CellFileInfo extends CellInfo {
    OutputStream output;
  }

  public GridRecordWriter(FileSystem outFileSystem, Path outFile, GridInfo gridInfo, CellInfo[] cells, boolean overwrite) throws IOException {
    this.gridInfo = gridInfo;
    LOG.info("Writing without RTree");
    this.fileSystem = outFileSystem;
    this.outFile = outFile;
    this.cells = cells;

    // Prepare arrays that hold streams
    cellStreams = new OutputStream[cells.length];

    Vector<Path> filesToOverwrite = new Vector<Path>();
    
    if (outFileSystem.exists(outFile)) {
        filesToOverwrite.add(outFile);
    }
    
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      Path cellFilePath = getCellFilePath(cellIndex);
      if (outFileSystem.exists(cellFilePath) && overwrite)
        filesToOverwrite.add(cellFilePath);
      
      LOG.info("Partitioning according to cell: " + cells[cellIndex]);
    }
    if (!overwrite && !filesToOverwrite.isEmpty()) {
      throw new RuntimeException("Cannot overwrite existing files: "+filesToOverwrite);
    }
    for (Path fileToOverwrite : filesToOverwrite)
      outFileSystem.delete(fileToOverwrite, true);
    
    text = new Text();
  }
  
  public synchronized void write(LongWritable dummyId, TigerShape shape) throws IOException {
    text.clear();
    shape.toText(text);
    write(dummyId, shape, text);
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
  public synchronized void write(LongWritable dummyId, TigerShape shape, Text text) throws IOException {
    // Write to all possible grid cells
    Rectangle mbr = shape.getMBR();
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      if (mbr.isIntersected(cells[cellIndex])) {
        writeToCell(cellIndex, text);
      }
    }
  }
  
  /**
   * A low level method to write a text to a specified cell.
   * 
   * @param cellIndex
   * @param text
   * @throws IOException
   */
  private synchronized void writeToCell(int cellIndex, Text text) throws IOException {
    OutputStream cellStream = getCellStream(cellIndex);
    cellStream.write(text.getBytes(), 0, text.getLength());
    cellStream.write(NEW_LINE);
  }
  
  // Get a stream that writes to the given cell
  private OutputStream getCellStream(int cellIndex) {
    if (cellStreams[cellIndex] == null) {
      try {
        // Try to get a stream that writes directly to the file
        cellStreams[cellIndex] = createCellFileStream(cellIndex);
      } catch (IOException e) {
        LOG.info("Cannot get a file handle. Using a temporary in memory stream instead");
        // Cannot open more files. May be out of handles.
        // Create a temporary memory-resident stream
        cellStreams[cellIndex] = new ByteArrayOutputStream();
      }
    }
    return cellStreams[cellIndex];
  }
  
  private OutputStream createCellFileStream(int cellIndex) throws IOException {
    OutputStream cellStream;
    Path cellFilePath = getCellFilePath(cellIndex);
    if (!fileSystem.exists(cellFilePath)) {
      // Create new file
      cellStream = fileSystem.create(cellFilePath, cells[cellIndex]);
    } else {
      // Append to existing file
      cellStream = fileSystem.append(cellFilePath);
    }
    return cellStream;
  }

  /**
   * Write the given shape to a specific cell. The shape is not replicated to any other cells.
   * It's just written to the given cell. This is useful when shapes are already assigned
   * and replicated to grid cells another way, e.g. from a map phase that partitions.
   * @param cellInfo
   * @param rect
   * @throws IOException
   */
  public synchronized void write(CellInfo cellInfo, TigerShape rect) throws IOException {
    text.clear();
    rect.toText(text);
    write(cellInfo, rect, text);
  }
  
  public synchronized void write(CellInfo cellInfo, TigerShape rect, Text text) throws IOException {
    // Write to the cell given
    int cellIndex = locateCell(cellInfo);
    writeToCell(cellIndex, text);
  }
  
  public synchronized void write(CellInfo cellInfo, Text text) throws IOException {
    // Write to the cell given
    int cellIndex = locateCell(cellInfo);
    writeToCell(cellIndex, text);
  }

  private int locateCell(CellInfo cellInfo) {
    // TODO use a hashtable for faster locating a cell
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++)
      if (cells[cellIndex].equals(cellInfo))
        return cellIndex;
    LOG.info("Could not find: "+cellInfo);
    LOG.info("In this list");
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      LOG.info("#"+cellIndex+": " + cells[cellIndex]);
    }
    return -1;
  }

  public synchronized void close() throws IOException {
    final Vector<Path> pathsToConcat = new Vector<Path>();
    Thread[] closingThreads = new Thread[cells.length];
    
    // Close all output files
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      final int iCell = cellIndex;
      closingThreads[cellIndex] = new Thread() {
        @Override
        public void run() {
          try {
            flushCell(iCell);
            finalizeCell(iCell);
            
            Path cellPath = getCellFilePath(iCell);
            if (fileSystem.exists(cellPath))
              pathsToConcat.add(cellPath);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      closingThreads[cellIndex].start();
    }
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      try {
        closingThreads[cellIndex].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    LOG.info("Closing... Merging "+pathsToConcat.size());
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

  /**
   * Flush current buffer to the given cell and reset buffer.
   * @param cellIndex
   * @throws IOException
   */
  private void flushCell(int cellIndex) throws IOException {
    OutputStream cellStream = cellStreams[cellIndex];
    // Flush only needed for temporary byte array outputstream
    if (cellStream == null || !(cellStream instanceof ByteArrayOutputStream))
      return;
    cellStream.close();
    byte[] bytes = ((ByteArrayOutputStream) cellStream).toByteArray();

    cellStreams[cellIndex] = createCellFileStream(cellIndex);
    cellStreams[cellIndex].write(bytes);
  }

  // Create a buffer filled with new lines
  final static byte[] buffer = new byte[1024 * 1024];
  static {
    Arrays.fill(buffer, (byte)'\n');
  }
  /**
   * Finalize the given cell file and makes it ready to be concatenated with
   * other cell files. This function should be called at the very end. Once
   * called, you should not append any more data to this cell.
   * @param cellIndex - The index of the cell to be written
   * @param buffer - A buffer used to fill the cell file until it reaches a 
   * size that is multiple of block size
   * @throws IOException
   */
  private void finalizeCell(int cellIndex) throws IOException {

    long blockSize = fileSystem.getDefaultBlockSize();

    if (fileSystem.exists(getCellFilePath(cellIndex))) {
      OutputStream cellStream = cellStreams[cellIndex];
      // I don't want to use getFileSize here because the file might still be
      // in memory (not closed) and I'm not sure it's legal to use getFileSize
      long cellSize = ((FSDataOutputStream)cellStream).getPos();
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
      // Now getFileSize should work because the file is closed
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
  
  @Override
  public void write(CellInfo cellInfo, BytesWritable buffer) throws IOException {
    throw new RuntimeException("Not supported");
  }
}
