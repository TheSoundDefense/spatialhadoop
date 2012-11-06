package org.apache.hadoop.spatial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RTreeGridRecordWriter<S extends Shape> extends GridRecordWriter<S> {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  
  /**Temporary streams to cells for writing element data*/
  protected OutputStream[] tempCellStreams;

  /**Keeps the number of elements written to each cell so far*/
  private int[] cellCount;
  /**The required degree of the rtree to be built*/
  private final int rtreeDegree;
  /**
   * Maximum number of elements to be written to the RTree so that it fits
   * the block.
   * TODO change this to hold the maximum total size in bytes for elements
   */
  private long rtreeLimit;
  
  /**Whether to use the fast mode for building RTree or not*/
  protected boolean fastRTree;
  
  /**
   * Maximum size of an RTree. Written files should have this as a block size.
   * Once number of records reach the maximum limit of this block size, records
   * are written as an RTree and the block is sealed. Next records will be
   * written in another block. This means that an RTree always starts at a block
   * boundary
   */
  private long blockSize;

  public RTreeGridRecordWriter(FileSystem fileSystem, Path outFile,
      CellInfo[] cells, boolean overwrite) throws IOException {
    super(fileSystem, outFile, cells, overwrite);
    LOG.info("Writing to RTrees");

    // Initialize the counters for each cell
    cellCount = new int[cells.length];
    tempCellStreams = new OutputStream[cells.length];
    
    // Determine the size of each RTree to decide when to flush a cell
    this.rtreeDegree = fileSystem.getConf().getInt(SpatialSite.RTREE_DEGREE, 11);
    this.blockSize = fileSystem.getConf().getLong(SpatialSite.RTREE_BLOCK_SIZE,
        fileSystem.getDefaultBlockSize());
    this.fastRTree = fileSystem.getConf().get(SpatialSite.RTREE_BUILD_MODE, "fast").equals("fast");
  }
  
  public void setStockObject(S stockObject) {
    super.setStockObject(stockObject);
    // The 8 is subtracted because we reserve it for the RTreeFileMarker
    this.rtreeLimit = RTree.getBlockCapacity(this.blockSize - 8, rtreeDegree,
        calculateRecordSize(stockObject));
  }
  
  /**
   * Calculates number of bytes needed to serialize an instance of the given
   * class. It writes a dummy object to a ByteArrayOutputStreams and calculates
   * number of bytes reserved by this stream.
   * @param klass
   * @return
   */
  public static int calculateRecordSize(Writable s) {
    int size = -1;
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bout);
      s.write(out);
      out.close();
      size = bout.toByteArray().length;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return size;
  }

  @Override
  protected synchronized void writeInternal(int cellIndex, S shape, Text text)
      throws IOException {
    DataOutput cellOutput = getTempCellStream(cellIndex);
    shape.write(cellOutput);
    
    // Write current contents as an RTree if reaches the limit of one RTree
    if (++cellCount[cellIndex] == rtreeLimit) {
      flushCell(cellIndex);
    }
  }
  
  @Override
  protected boolean isCellEmpty(int cellIndex) {
    return super.isCellEmpty(cellIndex) && tempCellStreams[cellIndex] == null;
  }

  @Override
  protected int getMaxConcurrentThreads() {
    // Since the closing cell is memory intensive, limit it to one
    return 1;
  }
  
  @Override
  protected void flushCell(int cellIndex) throws IOException {
    LOG.info("Writing the RTree at cell #"+cellIndex);
    if (tempCellStreams[cellIndex] == null)
      return;
    // Read element data
    tempCellStreams[cellIndex].close();
    tempCellStreams[cellIndex] = null;
    int fileSize = (int) fileSystem.getFileStatus(
        getTempCellFilePath(cellIndex)).getLen();
    FSDataInputStream cellIn = fileSystem.open(getTempCellFilePath(cellIndex));
    byte[] cellData = new byte[fileSize];
    cellIn.read(cellData, 0, fileSize);
    cellIn.close();
    fileSystem.delete(getTempCellFilePath(cellIndex), false);
    
    // Create the RTree using the element data
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject(stockObject);
    FSDataOutputStream cellStream = getCellStream(cellIndex);
    cellStream.writeLong(SpatialSite.RTreeFileMarker);
    rtree.bulkLoadWrite(cellData, 0, fileSize, rtreeDegree, cellStream, fastRTree);
    cellData = null;
    long blockSize =
        fileSystem.getFileStatus(getCellFilePath(cellIndex)).getBlockSize();
    // Stuff the file with bytes to make a complete block
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
    LOG.info("Size after writing the cell: "+cellStream.getPos());
    // Clean up after writing each cell as the code is heavy in memory
    System.gc();
  }

  @Override
  protected void finalizeCell(int cellIndex) throws IOException {
    // Close the cellStream if still open
    if (cellStreams[cellIndex] != null) {
      cellStreams[cellIndex].close();
      LOG.info("Final size: "+
          fileSystem.getFileStatus(getCellFilePath(cellIndex)).getLen());
      cellStreams[cellIndex] = null;
    }
  }
  
  @Override
  protected FSDataOutputStream getCellStream(int cellIndex) throws IOException {
    if (cellStreams[cellIndex] == null) {
      Path cellFilePath = getCellFilePath(cellIndex);
      if (!fileSystem.exists(cellFilePath)) {
        // Create new file
        cellStreams[cellIndex] = fileSystem.create(cellFilePath, true,
            fileSystem.getConf().getInt("io.file.buffer.size", 4096),
            fileSystem.getDefaultReplication(), this.blockSize,
            cells[cellIndex]);
      } else {
        // Append to existing file
        cellStreams[cellIndex] = fileSystem.append(cellFilePath);
      }
    }
    return (FSDataOutputStream) cellStreams[cellIndex];
  }
  
  /**
   * Return path to a temp file used to write element data before writing
   * the tree
   * @param column
   * @param row
   * @return
   */
  protected Path getTempCellFilePath(int cellIndex) {
    return new Path(outFile.toUri().getPath() + '_' + cellIndex +".tmp");
  }
  
  /**
   * Creates a temporary file used to write element data to of a cell
   * @param cellIndex
   * @return
   * @throws IOException
   */
  protected OutputStream createTempCellStream(int cellIndex) throws IOException {
    // Create new file
    return fileSystem.create(getTempCellFilePath(cellIndex), true);
  }
  
  protected FSDataOutputStream getTempCellStream(int cellIndex) throws IOException {
    if (tempCellStreams[cellIndex] == null) {
      // Create new file
      tempCellStreams[cellIndex] = createTempCellStream(cellIndex);
    }
    return (FSDataOutputStream) tempCellStreams[cellIndex];
  }
  
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path outFile = new Path("/media/scratch/test.rtree");
    GridInfo gridInfo = new GridInfo(0, 0, 1000000, 100000);
    gridInfo.columns = 16;
    gridInfo.rows = 16;
    CellInfo[] cells = gridInfo.getAllCells();
    RTreeGridRecordWriter<Rectangle> recordWriter = new RTreeGridRecordWriter<Rectangle>(fs, outFile,
        cells, true);
    recordWriter.setStockObject(new Rectangle());
    long rtreeLimit = recordWriter.rtreeLimit;
    long recordCount = rtreeLimit * 9 / 10;
    Random random = new Random();
    System.out.println("Creating "+recordCount+" records");
    long t1 = System.currentTimeMillis();
    Rectangle s = new Rectangle();
    for (CellInfo cellInfo : cells) {
      Rectangle mbr = cellInfo;
      for (int i = 0; i < recordCount; i++) {
        // Generate a random rectangle
        s.x = Math.abs(random.nextLong() % mbr.width) + mbr.x;
        s.y = Math.abs(random.nextLong() % mbr.height) + mbr.y;
        s.width = Math.min(Math.abs(random.nextLong() % 100) + 1,
            mbr.width + mbr.x - s.x);
        s.height = Math.min(Math.abs(random.nextLong() % 100) + 1,
            mbr.height + mbr.y - s.y);
        
        recordWriter.write(cellInfo, s);
      }
      recordWriter.write(cellInfo, null);
    }
    recordWriter.close();
    long t2 = System.currentTimeMillis();
    System.out.println("Finished in "+(t2-t1)+" millis");
    //System.out.println("Final size: "+fs.getFileStatus(outFile).getLen());
  }

}
