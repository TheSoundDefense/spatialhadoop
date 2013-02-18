package org.apache.hadoop.spatial;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class RTreeGridRecordWriter<S extends Shape> extends GridRecordWriter<S> {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  
  /**
   * Keeps the number of elements written to each cell so far.
   * Helps calculating the overhead of RTree indexing
   */
  private int[] cellCount;

  /**
   * The preferred rtreeDegree by the user. The constructed Rtree can be of,
   * at most, this degree. It can freely go under this value.
   */
  private final int preferredRtreeDegree;
  
  /**
   * Whether to use the fast mode for building RTree or not.
   * @see RTree#bulkLoadWrite(byte[], int, int, int, java.io.DataOutput, boolean)
   */
  protected boolean fastRTree;
  
  /**The maximum storage (in bytes) that can be accepted by the user*/
  protected int maximumStorageOverhead;

  /**
   * Initializes a new RTreeGridRecordWriter.
   * @param fileSystem - of output file
   * @param outFile - output file path
   * @param cells - the cells used to partition the input
   * @param overwrite - whether to overwrite existing files or not
   * @throws IOException
   */
  public RTreeGridRecordWriter(FileSystem fileSystem, Path outFile,
      CellInfo[] cells, boolean overwrite) throws IOException {
    super(fileSystem, outFile, cells, overwrite);
    LOG.info("Writing to RTrees");

    // Initialize the counters for each cell
    cellCount = new int[cells.length];
    
    // Determine the size of each RTree to decide when to flush a cell
    Configuration conf = fileSystem.getConf();
    this.preferredRtreeDegree = conf.getInt(SpatialSite.RTREE_DEGREE, 25);
    this.fastRTree = conf.get(SpatialSite.RTREE_BUILD_MODE, "fast").equals("fast");
    this.maximumStorageOverhead =
        (int) (conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f) * blockSize);
  }
  
  @Override
  protected synchronized void writeInternal(int cellIndex, Text text)
      throws IOException {
    if (text.getLength() == 0) {
      // Write to parent cell which will close the index
      super.writeInternal(cellIndex, text);
      return;
    }
    FSDataOutputStream cellOutput = (FSDataOutputStream) getCellStream(cellIndex);

    // Check if inserting this object will increase the degree of the R-tree
    // above the threshold
    int new_data_size =
        (int) (cellOutput.getPos() + text.getLength() + NEW_LINE.length);
    int bytes_available = (int) (blockSize - 8 - new_data_size);
    if (bytes_available < maximumStorageOverhead) {
      // Check if we need to flush current data
      int degree = RTree.findBestDegree(bytes_available, cellCount[cellIndex] + 1);
      if (degree > preferredRtreeDegree) {
        LOG.info("Early flushing an RTree with data "+cellOutput.getPos());
        // Writing this element will get the degree above the threshold
        // Flush current file and start a new file
        super.writeInternal(cellIndex, new Text());
      }
    }
    
    super.writeInternal(cellIndex, text);
    cellCount[cellIndex]++;
  }
  
  @Override
  protected int getMaxConcurrentThreads() {
    // Since the closing cell is memory intensive, limit it to one
    return 1;
  }


  @Override
  protected void closeCell(int cellIndex, boolean background)
      throws IOException {
    super.closeCell(cellIndex, background);
    cellCount[cellIndex] = 0;
  }
  
  /**
   * Closes a cell by writing all outstanding objects and closing current file.
   * Then, the file is read again, an RTree is built on top of it and, finally,
   * the file is written again with the RTree built.
   */
  @Override
  protected void closeCell(Path cellFilePath, OutputStream tempCellStream) throws IOException {
    // close stream to current file.
    // PS: No need to stuff it with new lines as it is only a temporary file
    tempCellStream.close();
    
    // Read all data of the written file in memory
    
    FileStatus fileStatus = fileSystem.getFileStatus(cellFilePath);
    long length = fileStatus.getLen();
    byte[] cellData = new byte[(int) length];
    InputStream cellIn = fileSystem.open(cellFilePath);
    cellIn.read(cellData);
    cellIn.close();
    // Get cell info of the file to use it when writing the RTree file
    CellInfo cellInfo = fileSystem.getFileBlockLocations(fileStatus, 0, 1)[0]
        .getCellInfo();
    // Delete the file to be able recreate it when written as an RTree
    fileSystem.delete(cellFilePath, true);
    
    // Build an RTree over the elements read from file
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject(stockObject);
    // It should create a new stream
    FSDataOutputStream cellStream =
      (FSDataOutputStream) createCellStream(cellFilePath, cellInfo);
    cellStream.writeLong(SpatialSite.RTreeFileMarker);
    rtree.bulkLoadWrite(cellData, 0, (int) length,
        (int) (blockSize - length - 8), cellStream, fastRTree);
    cellData = null; // To allow GC to collect it
    // Call the parent implementation which will stuff it with new lines
    super.closeCell(cellFilePath, cellStream);
  }
  
}
