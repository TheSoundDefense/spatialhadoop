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
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.RTreeGridRecordWriter;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;


/**
 * Reads a file as a list of RTrees
 * @author eldawy
 *
 */
public class RTreeRecordReader  implements RecordReader<CellInfo, RTree<TigerShape>>{
  public static final Log LOG = LogFactory.getLog(RTreeRecordReader.class);
  
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private FSDataInputStream fileIn;
  private final Seekable filePosition;
  private CompressionCodec codec;
  private Decompressor decompressor;
  
  private static String TigerShapeClassName;
  private static String ShapeClassName;
  private int index;

  private FileSystem fs;

  private Path file;

  public RTreeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    TigerShapeClassName = job.get(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShape.class.getName());
    ShapeClassName = job.get(TigerShapeRecordReader.SHAPE_CLASS, Point.class.getName());
    
    start = split.getStart();
    end = start + split.getLength();
    this.file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    this.fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
          ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK);
        fileIn = new FSDataInputStream(cIn);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn; // take pos from compressed stream
      } else {
        fileIn = new FSDataInputStream(codec.createInputStream(fileIn, decompressor));
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      filePosition = fileIn;
    }
    
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {
      // TODO skip until the beginning of the next RTree (how to know this?)
    }

    this.pos = start;
  }

  public RTreeRecordReader(Configuration job, FileSplit split, int index)
  throws IOException {
    this(job, split);
    this.index = index;
  }

  @Override
  public boolean next(CellInfo key, RTree<TigerShape> value) throws IOException {
    long marker = fileIn.readLong();
    if (marker != RTreeGridRecordWriter.RTreeFileMarker) {
      return false;
    }
    long start = this.pos;
    // Access RTree on disk
    //value.setInputStream(fileIn);
    // Read the entire RTree in memory
    value.readFields(fileIn);
    this.pos = fileIn.getPos();
    long len = this.pos - start;
    BlockLocation[] blockLocations = fs.getFileBlockLocations(file, start, len);
    CellInfo cellInfo = blockLocations[0].getCellInfo();
    if (cellInfo != null)
      key.set(cellInfo.x, cellInfo.y, cellInfo.width, cellInfo.height);
    else
      key.set(0, 0, 0, 0);
    return true;
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public RTree<TigerShape> createValue() {
    try {
      final Class<TigerShape> tigerShapeClass = (Class<TigerShape>) Class.forName(TigerShapeClassName);
      final Class<Shape> shapeClass = (Class<Shape>)Class.forName(ShapeClassName);

      RTree<TigerShape> rtree = new RTree<TigerShape>();
      try {
        Shape shape = shapeClass.newInstance();
        TigerShape tigerShape = tigerShapeClass.newInstance();
        tigerShape.shape = shape;
        rtree.setStockObject(tigerShape);
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
