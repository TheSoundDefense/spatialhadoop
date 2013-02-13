package org.apache.hadoop.mapred.spatial;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

/**
 * A base class to read shapes from files. It reads either single shapes,
 * list of shapes, or rtrees. It automatically detects the format of the
 * underlying block and parses it accordingly.
 * @author eldawy
 *
 */
public abstract class SpatialRecordReader<K, V> implements RecordReader<K, V> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader.class);
  
  /**Maximum number of shapes to read in one operation to return as array*/
  private int maxShapesInRead;
  
  enum BlockType { HEAP, RTREE};
  
  /** First offset that is read from the input */
  protected long start;
  /** Last offset to stop at */
  protected long end;
  /** Position of the next byte to read/prase from the input */
  protected long pos;
  /** Input stream that reads from file */
  private InputStream in;
  /** Reads lines from text files */
  protected LineReader lineReader;
  /** A temporary text to read lines from lineReader */
  protected Text tempLine = new Text();
  /** Some bytes that were read from the stream but not parsed yet */
  protected byte[] buffer;

  /** File system of the file being parsed */
  private FileSystem fs;

  /** The path of the parsed file */
  private Path path;

  /** Block size for the read file. Used with RTrees */
  protected long blockSize;

  /** A cached value for the current cellInfo */
  protected CellInfo cellInfo;

  /**The type of the currently parsed block*/
  protected BlockType blockType;
  
  /**
   * Initialize from an input split
   * @param split
   * @param conf
   * @param reporter
   * @param index
   * @throws IOException
   */
  public SpatialRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    this(conf, split.getStartOffsets()[index], split.getLength(index),
        split.getPath(index));
  }
  
  /**
   * Initialize from a FileSplit
   * @param job
   * @param split
   * @throws IOException
   */
  public SpatialRecordReader(Configuration job, FileSplit split) throws IOException {
    this(job, split.getStart(), split.getLength(), split.getPath());
  }

  /**
   * Initialize from a path and range
   * @param job
   * @param s
   * @param l
   * @param p
   * @throws IOException
   */
  public SpatialRecordReader(Configuration job, long s, long l, Path p) throws IOException {
    this.start = s;
    this.end = s + l;
    this.path = p;
    this.fs = this.path.getFileSystem(job);
    this.in = fs.open(this.path);
    this.blockSize = fs.getFileStatus(this.path).getBlockSize();
    
    final CompressionCodec codec = new CompressionCodecFactory(job).getCodec(this.path);

    if (codec != null) {
      // Decompress the stream
      in = codec.createInputStream(in);
      // Read till the end of the stream
      end = Long.MAX_VALUE;
    } else {
      ((FSDataInputStream)in).seek(start);
    }
    this.pos = start;
    this.maxShapesInRead = job.getInt(SpatialSite.MaxShapesInOneRead, 1000000);
  }
  
  /**
   * Construct from an input stream already set to the first byte
   * to read.
   * @param in
   * @param offset
   * @param endOffset
   * @throws IOException
   */
  public SpatialRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    this.in = in;
    this.start = offset;
    this.end = endOffset;
    this.pos = offset;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void close() throws IOException {
    if (lineReader != null) {
      lineReader.close();
    } else if (in != null) {
      in.close();
    }
    lineReader = null;
    in = null;
  }

  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  /**
   * @throws IOException
   */
  protected boolean moveToNextBlock() throws IOException {
    // Caution this method is called at the very beginning with pos points
    // to the start position but the file was never read
    long new_pos = getPos();
    if (blockSize != 0 && getPos() % blockSize > 0) {
      // Currently in the middle of a block, move to the beginning of next block
      new_pos = getPos() + blockSize - (getPos() % blockSize);
    }
    if (new_pos >= end)
      return false;
    // Seek to the start of the needed block
    if (in instanceof Seekable) {
      ((Seekable)in).seek(new_pos);
    } else {
      in.skip(new_pos - pos);
    }
    pos = new_pos;
    
    // Read the first part of the block to determine its type
    buffer = new byte[8];
    in.read(buffer);
    if (Arrays.equals(buffer, SpatialSite.RTreeFileMarkerB)) {
      LOG.info("Block is RTree indexed at position "+start);
      blockType = BlockType.RTREE;
      pos += 8;
    } else {
      blockType = BlockType.HEAP;
      // The read buffer might contain some data that must be read
      // File is text file
      lineReader = new LineReader(in);
  
      // Skip the first line unless we are reading the first block in file
      boolean skipFirstLine = getPos() != 0;
      if (skipFirstLine) {
        // Search for the first occurrence of a new line
        int eol = RTree.skipToEOL(buffer, 0);
        // If we found an end of line in the buffer, we do not need to skip
        // a line from the open stream. This happens if the EOL returned is
        // beyond the end of buffer and the buffer is not a complete line
        // by itself
        boolean skip_another_line_from_stream = eol >= buffer.length &&
            buffer[buffer.length - 1] != '\n';
        if (eol < buffer.length) {
          // Found an EOL in the buffer and there are some remaining bytes
          byte[] tmp = new byte[buffer.length - eol];
          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
          buffer = tmp;
          // Advance current position to skip the first partial line
          this.pos += eol;
        } else {
          // Did not find an EOL in the buffer or found it at the very end
          pos += buffer.length;
          // Buffer does not contain any useful data
          buffer = null;
        }
        
        if (skip_another_line_from_stream) {
          // Didn't find an EOL in the buffer, need to skip it from the stream
          pos += lineReader.readLine(tempLine, 0, (int)(end - pos));
        }
      }
    }
    
    // Get the cell info for this block
    if (path != null) {
      BlockLocation[] blockLocations =
          fs.getFileBlockLocations(fs.getFileStatus(path), pos, 1);
      cellInfo = blockLocations[0].getCellInfo();
    }
    
    return true;
  }

  /**
   * Reads the next line from input and return true if a line was read.
   * If no more lines are available in this split, a false is returned.
   * @param value
   * @return
   * @throws IOException
   */
  protected boolean nextLine(Text value, boolean moveAcrossBlocks) throws IOException {
    if (blockType == null)
      moveToNextBlock();
    while (getPos() <= end) {
      value.clear();
      if (buffer != null) {
        // Read the first line encountered in buffer
        int eol = RTree.skipToEOL(buffer, 0);
        value.append(buffer, 0, eol);
        if (eol < buffer.length) {
          // There are still some bytes remaining in buffer
          byte[] tmp = new byte[buffer.length - eol];
          System.arraycopy(buffer, eol, tmp, 0, tmp.length);
        } else {
          buffer = null;
        }
        // Check if a complete line has been read from the buffer
        byte last_byte = value.getBytes()[value.getLength()-1];
        if (last_byte == '\n' || last_byte == '\r')
          return true;
      }
      
      // Read the first line from stream
      Text temp = new Text();
      int b = lineReader.readLine(temp);
      pos += b;
      
      // Append the part read from stream to the part extracted from buffer
      value.append(temp.getBytes(), 0, temp.getLength());
      
      if (value.getLength() > 1) {
        // Read a non-empty line. Note that end-of-line character is included
        return true;
      }
      // Line read is empty or contains only the new line character
      if (!moveAcrossBlocks)
        return false;
      
      // Reached end of block, move to next block and check for end of file
      if (!moveToNextBlock())
        return false;
      
      // Skip the header of the tree as we are concerned in data lines
      if (blockType == BlockType.RTREE)
        pos += RTree.skipHeader(in);
      // Reinitialize record reader at the new position
      lineReader = new LineReader(in);
    }
    // Reached end of file
    return false;
  }

  /**
   * Reads next shape from input and returns true. If no more shapes are left
   * in the split, a false is returned.
   * @param s
   * @return
   * @throws IOException 
   */
  protected boolean nextShape(Shape s, boolean moveAcrossBlocks) throws IOException {
    if (!nextLine(tempLine, moveAcrossBlocks))
      return false;
    s.fromText(tempLine);
    return true;
  }
  
  /**
   * Reads all shapes left in the current block in one shot.
   * @param shapes
   * @return
   * @throws IOException
   */
  protected boolean nextShapes(ArrayWritable shapes) throws IOException {
    // Prepare a vector that will hold all objects in this 
    Vector<Shape> vshapes = new Vector<Shape>();
    try {
      Shape stockObject = (Shape) shapes.getValueClass().newInstance();
      // Reached the end of this split
      if (getPos() >= end)
        return false;
      
      // Read all shapes in this block
      while (nextShape(stockObject, true) && vshapes.size() < maxShapesInRead) {
        vshapes.add(stockObject.clone());
      }

      // Store them in the return value
      shapes.set(vshapes.toArray(new Shape[vshapes.size()]));
      
      return vshapes.size() > 0;
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    } catch (OutOfMemoryError e) {
      LOG.error("Error reading shapes. Stopped with "+vshapes.size()+" shapes");
      throw e;
    }
    return false;
  }
  
  /**
   * Reads the next RTree from file.
   * @param rtree
   * @return
   * @throws IOException
   */
  protected boolean nextRTree(RTree<? extends Shape> rtree) throws IOException {
    if (getPos() >= end || !moveToNextBlock())
      return false;
    if (blockType == BlockType.RTREE) {
      // Signature was already read in initialization.
      buffer = null;
      DataInput dataIn = in instanceof DataInput?
          (DataInput) in : new DataInputStream(in);
      rtree.readFields(dataIn);
      return true;
    } else {
      throw new RuntimeException("Not implemented");
    }
  }
}
