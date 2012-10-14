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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class GridRecordWriter implements ShapeRecordWriter {
  public static final Log LOG = LogFactory.getLog(GridRecordWriter.class);
  /**An output stream for each grid cell*/
  protected CellInfo[] cells;
  protected OutputStream[] cellStreams;
  protected final Path outFile;
  protected final FileSystem fileSystem;
  private Text text;

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

  public GridRecordWriter(FileSystem outFileSystem, Path outFile,
      CellInfo[] cells, boolean overwrite) throws IOException {
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
  
  @Override
  public synchronized void write(LongWritable dummyId, Shape shape) throws IOException {
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
  @Override
  public synchronized void write(LongWritable dummyId, Shape shape, Text text) throws IOException {
    // Write to all possible grid cells
    Rectangle mbr = shape.getMBR();
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      if (mbr.isIntersected(cells[cellIndex])) {
        writeInternal(cellIndex, shape, text);
      }
    }
  }
  
  /**
   * Write the given shape to a specific cell. The shape is not replicated to any other cells.
   * It's just written to the given cell. This is useful when shapes are already assigned
   * and replicated to grid cells another way, e.g. from a map phase that partitions.
   * @param cellInfo
   * @param shape
   * @throws IOException
   */
  @Override
  public synchronized void write(CellInfo cellInfo, Shape shape) throws IOException {
    if (shape == null) {
      closeCell(locateCell(cellInfo));
      return;
    }
    text.clear();
    shape.toText(text);
    write(cellInfo, shape, text);
  }

  @Override
  public synchronized void write(CellInfo cellInfo, Shape shape, Text text) throws IOException {
    // Write to the cell given
    int cellIndex = locateCell(cellInfo);
    writeInternal(cellIndex, shape, text);
  }

  /**
   * Write the given shape to the cellIndex indicated.
   * @param cellIndex
   * @param shape
   * @throws IOException
   */
  protected synchronized void writeInternal(int cellIndex, Shape shape, Text text) throws IOException {
    if (text == null) {
      text = this.text;
      shape.toText(text);
    }
    OutputStream cellStream = getCellStream(cellIndex);
    cellStream.write(text.getBytes(), 0, text.getLength());
    cellStream.write(NEW_LINE);
  }
  
  // Get a stream that writes to the given cell
  protected OutputStream getCellStream(int cellIndex) throws IOException {
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

  protected int locateCell(CellInfo cellInfo) {
    // TODO use a hashtable for faster locating of a cell
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

  protected boolean isCellEmpty(int cellIndex) {
    try {
      return cellStreams[cellIndex] == null
          && !fileSystem.exists(getCellFilePath(cellIndex));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }
  
  /**
   * Close the whole writer. Finalize all cell files and concatenate them
   * into the output file.
   */
  public synchronized void close() throws IOException {
    final Vector<Path> pathsToConcat = new Vector<Path>();
    final int MaxConcurrentThreads = 10;
    final Vector<Thread> closingThreads = new Vector<Thread>();
    
    // Close all output files
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      if (!isCellEmpty(cellIndex)) {
        final int iCell = cellIndex;
        closingThreads.add(new Thread() {
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
        });
        closingThreads.lastElement().start();
        
        // Limit number of concurrent threads to save memory
        if (closingThreads.size() > MaxConcurrentThreads) {
          try {
            closingThreads.firstElement().join();
            closingThreads.remove(0);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    for (Thread closingThread : closingThreads) {
      try {
        closingThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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

  // Create a buffer filled with new lines
  final static byte[] buffer = new byte[1024 * 1024];

  static {
    Arrays.fill(buffer, (byte)'\n');
  }
  
  /**
   * Close the given cell freeing all memory reserved by it.
   * Once a cell is closed, we should not write more data to it.
   * @param cellInfo
   * @throws IOException
   */
  protected void closeCell(int cellIndex) throws IOException {
    // Flush all outstanding writes to the file
    flushCell(cellIndex);
    if (fileSystem.getConf().getBoolean("dfs.support.append", false)) {
      // Close the file. We can later append to it or finalize it
      if (cellStreams[cellIndex] != null) {
        cellStreams[cellIndex].close();
        cellStreams[cellIndex] = null;
      }
    } else {
      // File system doesn't support append. We need to finalize it right now
      finalizeCell(cellIndex);
    }
  }
  
  /**
   * Flush current buffer to the given cell and reset buffer.
   * @param cellIndex
   * @throws IOException
   */
  protected void flushCell(int cellIndex) throws IOException {
    OutputStream cellStream = cellStreams[cellIndex];
    // Flush only needed for temporary byte array outputstream
    if (cellStream == null || !(cellStream instanceof ByteArrayOutputStream))
      return;
    cellStream.close();
    byte[] bytes = ((ByteArrayOutputStream) cellStream).toByteArray();

    cellStreams[cellIndex] = createCellFileStream(cellIndex);
    cellStreams[cellIndex].write(bytes);
  }

  /**
   * Finalize the given cell file and makes it ready to be concatenated with
   * other cell files. This function should be called at the very end. Once
   * called, you should not append any more data to this cell.
   * If a stream is open to the given file, we use it to write empty lines to
   * the file to be a multiple of block size. If the file has been already
   * closed, we open it for append (if supported) and write empty lines.
   * If in the later case append is not supported, the method fails and an
   * IOException is thrown.
   * @param cellIndex - The index of the cell to be written
   * @param buffer - A buffer used to fill the cell file until it reaches a 
   * size that is multiple of block size
   * @throws IOException
   */
  protected void finalizeCell(int cellIndex) throws IOException {
    if (!fileSystem.exists(getCellFilePath(cellIndex)) &&
        cellStreams[cellIndex] == null)
      return;

    // Get current file size
    OutputStream cellStream = cellStreams[cellIndex];
    long currSize;
    // Check if the file was previously closed and need to be finalized
    if (cellStream == null) {
      currSize = fileSystem.getFileStatus(getCellFilePath(cellIndex)).getLen();
    } else {
      // I don't want to use getFileSize here because the file might still be
      // in memory (not closed) and I'm not sure it's legal to use getFileSize
      currSize = ((FSDataOutputStream)cellStream).getPos();
    }

    // Check if we need to write empty lines
    long blockSize =
        fileSystem.getFileStatus(getCellFilePath(cellIndex)).getBlockSize();
    LOG.info("Current size: "+currSize);
    // Stuff the open stream with empty lines until it becomes of size blockSize
    long remainingBytes = (blockSize - currSize % blockSize) % blockSize;
    
    if (remainingBytes > 0) {
      LOG.info("Stuffing file " + cellIndex +  " with new lines: " + remainingBytes);
      if (cellStream == null) {
        // Open for append. An exception is throws if FS doesn't support append
        cellStream = createCellFileStream(cellIndex);
      }
      // Write some bytes so that remainingBytes is multiple of buffer.length
      cellStream.write(buffer, 0, (int)(remainingBytes % buffer.length));
      remainingBytes -= remainingBytes % buffer.length;
      // Write chunks of size buffer.length
      while (remainingBytes > 0) {
        cellStream.write(buffer);
        remainingBytes -= buffer.length;
      }
    }
    if (cellStream != null) {
      // Close stream
      cellStream.close();
      cellStreams[cellIndex] = null;
    }
    // Now getFileSize should work because the file is closed
    LOG.info("Actual size: "+fileSystem.getFileStatus(getCellFilePath(cellIndex)).getLen());
  }

  /**
   * Return path to a file that is used to write one grid cell
   * @param column
   * @param row
   * @return
   */
  protected Path getCellFilePath(int cellIndex) {
    return new Path(outFile.toUri().getPath() + '_' + cellIndex);
  }
}
