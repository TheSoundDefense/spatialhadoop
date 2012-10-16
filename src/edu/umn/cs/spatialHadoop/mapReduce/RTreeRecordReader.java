package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.RTreeGridRecordWriter;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatialHadoop.TigerShapeWithIndex;


/**
 * Reads a file as a list of RTrees
 * @author eldawy
 *
 */
public class RTreeRecordReader  implements RecordReader<CellInfo, RTree<Shape>>{
  public static final Log LOG = LogFactory.getLog(RTreeRecordReader.class);
  
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  private CompressionCodec codec;
  private Decompressor decompressor;
  
  private static String ShapeClassName;
  private int index;

  private FileSystem fs;

  private Path file;

  public RTreeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    ShapeClassName = job.get(ShapeRecordReader.SHAPE_CLASS, TigerShape.class.getName());
    
    start = split.getStart();
    end = start + split.getLength();
    file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    this.fs = file.getFileSystem(job);
    fileIn = fs.open(split.getPath());
    if (codec != null) {
      fileIn = new FSDataInputStream(codec.createInputStream(fileIn));
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        fileIn.seek(start);
      }
    }
    this.filePosition = fileIn;
    
    this.pos = start;
  }

  public RTreeRecordReader(Configuration job, FileSplit split, int index)
  throws IOException {
    this(job, split);
    this.index = index;
  }

  @Override
  public boolean next(CellInfo key, RTree<Shape> value) throws IOException {
    if (fileIn.getPos() >= end)
      return false;
    LOG.info("Looking for an RTree at Pos: " + fileIn.getPos());
    long marker = fileIn.readLong();
    if (marker != RTreeGridRecordWriter.RTreeFileMarker) {
      LOG.info("No more RTrees stored in file");
      return false;
    }
    LOG.info("Found an RTree in file");
    // Find the CellInfo of the block where the RTree is stored
    BlockLocation[] blockLocations =
        fs.getFileBlockLocations(fs.getFileStatus(file), this.pos, 1);
    CellInfo cellInfo = blockLocations[0].getCellInfo();
    if (cellInfo != null)
      key.set(cellInfo.x, cellInfo.y, cellInfo.width, cellInfo.height);
    else
      key.set(0, 0, 0, 0);
    // Read RTree from disk
    value.readFields(fileIn);
    // Update position
    this.pos = fileIn.getPos();
    return true;
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public RTree<Shape> createValue() {
    try {
      final Class<? extends Shape> shapeClass =
          Class.forName(ShapeClassName).asSubclass(Shape.class);

      RTree<Shape> rtree = new RTree<Shape>();
      try {
        Shape shape = shapeClass.newInstance();
        if (shape instanceof TigerShapeWithIndex)
          ((TigerShapeWithIndex) shape).index = index;
        LOG.info("Stock object in the RTree is: "+shape);
        rtree.setStockObject(shape);
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      return rtree;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    throw new RuntimeException("TigerShape class not set appropriately in job");
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    try {
      if (fileIn != null) {
        fileIn.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }
  
  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }

  private boolean isCompressedInput() {
    return (codec != null);
  }

}
