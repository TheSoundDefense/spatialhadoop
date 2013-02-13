package org.apache.hadoop.spatial;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;

/**
 * Writes a spatial file where objects are of type S.
 * @author eldawy
 *
 * @param <S>
 */
public class GridRecordWriter<S extends Shape> implements ShapeRecordWriter<S> {
  public static final Log LOG = LogFactory.getLog(GridRecordWriter.class);
  /**The spatial boundaries for each cell*/
  protected CellInfo[] cells;
  
  /**An output stream for each grid cell*/
  protected OutputStream[] cellStreams;
  
  /**The path of open cell files*/
  protected Path[] cellFilePath;
  
  /**Paths that need to be concatenated upon writer close*/
  protected Vector<Path> pathsToConcat;

  /**Background threads that close cell files*/
  private Vector<Thread> closingThreads;

  /**Path of the output file*/
  protected final Path outFile;
  
  /**File system for output path*/
  protected final FileSystem fileSystem;

  /**Temporary text to serialize one object*/
  protected Text text;
  
  /**Block size for grid file written*/
  protected long blockSize;
  
  /**A stock object used for serialization/deserialization*/
  protected S stockObject;
  
  /**New line marker */
  protected static final byte[] NEW_LINE = {'\n'};
  
  public GridRecordWriter(FileSystem outFileSystem, Path outFile,
      CellInfo[] cells, boolean overwrite) throws IOException {
    this.fileSystem = outFileSystem;
    this.outFile = outFile;
    // Make sure cellIndex maps to array index. This necessary for calls that call directly
    // write(int, Text)
    int highest_index = 0;
    for (CellInfo cell : cells) {
      if (cell.cellId > highest_index)
        highest_index = (int) cell.cellId;
    }
    this.cells = new CellInfo[highest_index + 1];
    for (CellInfo cell : cells) {
      this.cells[(int) cell.cellId] = cell;
      LOG.info(cell.cellId+": "+cell);
    }
    
    // Prepare arrays that hold cells information
    cellStreams = new OutputStream[this.cells.length];
    cellFilePath = new Path[this.cells.length];
    pathsToConcat = new Vector<Path>();
    
    if (fileSystem.exists(outFile)) {
      if (!overwrite)
        throw new RuntimeException("File already exists and -overwrite flag is not set");
      fileSystem.delete(outFile, true);
    }

    this.blockSize = fileSystem.getConf().getLong(
        SpatialSite.LOCAL_INDEX_BLOCK_SIZE, fileSystem.getDefaultBlockSize());
    
    text = new Text();
  }
  
  public void setBlockSize(long _block_size) {
    this.blockSize = _block_size;
  }
  
  public void setStockObject(S stockObject) {
    this.stockObject = stockObject;
  }

  @Override
  public synchronized void write(NullWritable dummy, S shape) throws IOException {
    text.clear();
    if (shape != null)
      shape.toText(text);
    write(shape, text);
  }

  /**
   * Writes the given shape to the current grid file. The text representation that need
   * to be written is passed for two reasons.
   * 1 - Avoid converting the shape to text. (Performance)
   * 2 - Some information may have been lost from the original line when converted to shape
   * The text is assumed not to have a new line. This method writes a new line to the output
   * after the given text is written.
   * @param shape
   * @param text
   * @throws IOException
   */
  @Override
  public synchronized void write(S shape, Text text) throws IOException {
    // Write to all possible grid cells
    Rectangle mbr = shape.getMBR();
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      if (mbr.isIntersected(cells[cellIndex])) {
        writeInternal(cellIndex, text);
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
  public synchronized void write(CellInfo cellInfo, S shape) throws IOException {
    if (shape == null) {
      write(cellInfo, shape, null);
      return;
    }
    text.clear();
    shape.toText(text);
    write(cellInfo, shape, text);
  }

  @Override
  public synchronized void write(CellInfo cellInfo, S shape, Text text) throws IOException {
    // Write to the cell given
    writeInternal((int)cellInfo.cellId, text);
  }
  
  @Override
  public void write(int cellId, Text shapeText) throws IOException {
    this.writeInternal(cellId, shapeText);
  }

  /**
   * Write the given shape to the cellIndex indicated.
   * @param cellIndex
   * @param shape
   * @throws IOException
   */
  protected synchronized void writeInternal(int cellIndex, Text text) throws IOException {
    if (text.getLength() == 0) {
      closeCell(cellIndex, false);
    } else {
      OutputStream cellStream = getCellStream(cellIndex);
      cellStream.write(text.getBytes(), 0, text.getLength());
      cellStream.write(NEW_LINE);
    }
  }
  
  // Get a stream that writes to the given cell
  protected OutputStream getCellStream(int cellIndex) throws IOException {
    if (cellStreams[cellIndex] == null) {
      Path cellFilePath = getCellFilePath(cellIndex);
      cellStreams[cellIndex] = createCellStream(cellFilePath, cells[cellIndex]);
    }
    return cellStreams[cellIndex];
  }
  
  /**
   * Creates an output stream that will be used to write a cell file.
   * @param cellFilePath
   * @return
   * @throws IOException 
   */
  protected OutputStream createCellStream(Path cellFilePath, CellInfo cellInfo)
      throws IOException {
    OutputStream cellStream;
    if (!fileSystem.exists(cellFilePath)) {
      // Create new file
      cellStream = fileSystem.create(cellFilePath, true,
          fileSystem.getConf().getInt("io.file.buffer.size", 4096),
          fileSystem.getDefaultReplication(), this.blockSize,
          cellInfo);
    } else {
      // Append to existing file
      cellStream = fileSystem.append(cellFilePath);
    }
    return cellStream;
  }
  
  /**
   * Close a cell given an ID.
   * @param cellIndex
   * @param background
   * @throws IOException 
   */
  protected void closeCell(int cellIndex, boolean background) throws IOException {
    if (cellStreams[cellIndex] == null)
      return; // No cell to close
    if (background) {
      closingThreads.add(new CloseCell(cellFilePath[cellIndex], cellStreams[cellIndex]));
      refreshClosingThreads();
    } else {
      closeCell(cellFilePath[cellIndex], cellStreams[cellIndex]);
    }
    cellFilePath[cellIndex] = null;
    cellStreams[cellIndex] = null;
  }
  
  /**
   * Closes a cell file in the background
   * @author eldawy
   *
   */
  private class CloseCell extends Thread {
    /**The path of the file to close*/
    private Path cellFilePath;
    /**An output stream open on the cell file*/
    private OutputStream cellStream;

    public CloseCell(Path cellFilePath, OutputStream cellStream) {
      this.cellFilePath = cellFilePath;
      this.cellStream = cellStream;
    }
    
    @Override
    public void run() {
      try {
        closeCell(cellFilePath, cellStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Close the given cell freeing all memory reserved by it.
   * Once a cell is closed, we should not write more data to it.
   * @param cellInfo
   * @throws IOException
   */
  protected void closeCell(Path cellFilePath, OutputStream cellStream) throws IOException {
    // Get current file size
    long currSize = ((FSDataOutputStream)cellStream).getPos();

    // Check if we need to write empty lines
    FileStatus fileStatus = fileSystem.getFileStatus(cellFilePath);
    long blockSize = fileStatus.getBlockSize();

    LOG.info("Cell current size: "+currSize);
    // Stuff the open stream with empty lines until it becomes of size blockSize
    long remainingBytes = (blockSize - currSize % blockSize) % blockSize;
    
    if (remainingBytes > 0) {
      LOG.info("Cell stuffing file with "+remainingBytes+" new lines");
      // Write some bytes so that remainingBytes is multiple of buffer.length
      cellStream.write(buffer, 0, (int)(remainingBytes % buffer.length));
      remainingBytes -= remainingBytes % buffer.length;
      // Write chunks of size buffer.length
      while (remainingBytes > 0) {
        cellStream.write(buffer);
        remainingBytes -= buffer.length;
      }
    }
    // Close stream
    cellStream.close();
    // Now getFileSize should work because the file is closed
    LOG.info("Cell actual size: "
        + fileSystem.getFileStatus(cellFilePath).getLen());
  }
  
  /**
   * Close the whole writer. Finalize all cell files and concatenate them
   * into the output file.
   */
  public synchronized void close(Progressable progressable) throws IOException {
    closingThreads = new Vector<Thread>();
    
    // Close all output files
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      if (cellStreams[cellIndex] != null) {
        closeCell(cellIndex, true);
      }
    }

    do {
      // Ensure that there is at least MaxConcurrentThreads running
      refreshClosingThreads();
      // Indicate progress
      if (progressable != null)
        progressable.progress();
      if (!closingThreads.isEmpty()) {
        try {
          // Sleep for 10 seconds or until the first thread terminates
          closingThreads.firstElement().join(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } while (!closingThreads.isEmpty());

    if (pathsToConcat.size() == 0) {
      LOG.warn("No output of the grid file: "+outFile);
      return;
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

  /**
   * Refreshes the closing threads by doing: 1- Removing threads that are
   * already terminated. 2- Start idle threads while ensuring a maximum of
   * {@link #getMaxConcurrentThreads()} concurrent active threads
   */
  protected void refreshClosingThreads() {
    int i = 0;
    while (i < getMaxConcurrentThreads() && i < closingThreads.size()) {
      Thread.State state = closingThreads.elementAt(i).getState(); 
      if (state == Thread.State.TERMINATED) {
        // Thread already terminated, remove from the queue
        closingThreads.remove(i);
      } else if (state == Thread.State.NEW) {
        // Start the thread and move to next one
        closingThreads.elementAt(i++).start();
      } else {
        // Thread is still running, skip over it
        i++;
      }
    }
  }

  /**Returns maximum number of concurrent threads when closing the file*/
  protected int getMaxConcurrentThreads() {
    return 10;
  }

  // Create a buffer filled with new lines
  final static byte[] buffer = new byte[1024 * 1024];
  
  static {
    Arrays.fill(buffer, (byte)'\n');
  }

  /**
   * Return path to a file that is used to write one grid cell
   * @param column
   * @param row
   * @return
   * @throws IOException 
   */
  protected Path getCellFilePath(int cellIndex) throws IOException {
    if (cellFilePath[cellIndex] == null) {
      do {
        cellFilePath[cellIndex] = new Path(outFile.toUri().getPath() + '_'
            + (int)(Math.random() * 1000000));
      } while (fileSystem.exists(cellFilePath[cellIndex]));
      pathsToConcat.add(cellFilePath[cellIndex]);
    }
    return cellFilePath[cellIndex];
  }
}
