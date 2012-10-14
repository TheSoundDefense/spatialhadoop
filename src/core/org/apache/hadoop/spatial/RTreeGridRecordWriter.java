package org.apache.hadoop.spatial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RTreeGridRecordWriter extends GridRecordWriter {
  public static final Log LOG = LogFactory.getLog(RTreeGridRecordWriter.class);
  
  /**The default RTree degree used for local indexing*/
  public static final String RTREE_DEGREE = "spatialHadoop.storage.RTreeDegree";
  
  /**Maximum size of an RTree.*/
  public static final String RTREE_BLOCK_SIZE =
      "spatialHadoop.storage.RTreeBlockSize";
  
  static {
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
  /**
   * A marker put in the beginning of each block to indicate that this block
   * is stored as an RTree. It might be better to store this in the BlockInfo
   * in a field (e.g. localIndexType).
   */
  public static final long RTreeFileMarker = -0x0123456;

  /**An output stream for each grid cell*/
  private Vector<Shape>[] cellShapes;
  private final int rtreeDegree;
  private final long rtreeLimit;
  
  /**
   * Maximum size of an RTree. Written files should have this as a block size.
   * Once number of records reach the maximum limit of this block size, records
   * are written as an RTree and the block is sealed. Next records will be
   * written in another block. This means that an RTree always starts at a block
   * boundary
   */
  private long blockSize;
  
  @SuppressWarnings("unchecked")
  public RTreeGridRecordWriter(FileSystem fileSystem, Path outFile,
      CellInfo[] cells, boolean overwrite) throws IOException {
    super(fileSystem, outFile, cells, overwrite);
    LOG.info("Writing to RTrees");

    // Initialize the buffer that stores shapes to be stored in each cell
    cellShapes = new Vector[cells.length];
    for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
      cellShapes[cellIndex] = new Vector<Shape>();
    }
    
    // Determine the size of each RTree to decide when to flush a cell
    this.rtreeDegree = fileSystem.getConf().getInt(RTREE_DEGREE, 11);
    this.blockSize = fileSystem.getConf().getLong(RTREE_BLOCK_SIZE,
        fileSystem.getDefaultBlockSize());
    // The 8 is subtracted because we reserve it for the RTreeFileMarker
    this.rtreeLimit = RTree.getBlockCapacity(this.blockSize - 8, rtreeDegree,
        calculateRecordSize(TigerShape.class));
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

  @Override
  protected synchronized void writeInternal(int cellIndex, Shape shape, Text text)
      throws IOException {
    cellShapes[cellIndex].add(shape.clone());
    // Write current contents as an RTree if reaches the limit of one RTree
    if (cellShapes[cellIndex].size() == rtreeLimit) {
      flushCell(cellIndex);
    }
  }
  
  @Override
  protected boolean isCellEmpty(int cellIndex) {
    return super.isCellEmpty(cellIndex) && cellShapes[cellIndex].size() == 0;
  }
  
  @Override
  protected void flushCell(int cellIndex) throws IOException {
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
      long blockSize =
          fileSystem.getFileStatus(getCellFilePath(cellIndex)).getBlockSize();
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

      LOG.info("Actual size: "+cellStream.getPos());
    }
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
}
