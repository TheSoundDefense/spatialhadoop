package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.TigerShape;

/**
 * A base class to read shapes from files. It reads either single shapes,
 * list of shapes, or rtrees. It automatically detects the format of the
 * underlying block and parses it accordingly.
 * @author eldawy
 *
 */
public abstract class SpatialRecordReader<K, V> implements RecordReader<K, V> {
  private static final Log LOG = LogFactory.getLog(SpatialRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  /**Flag set to true if the file is indexed as an RTree*/
  protected boolean isRTree;
  protected long start;
  protected long pos;
  protected long end;
  /**Input stream that reads from file*/
  private FSDataInputStream in;
  /**Reads lines from text files*/
  protected LineReader lineReader;
  protected Text tempLine;
  int maxLineLength;
  protected byte[] signature;
  /**Number of elements in the RTree currently being read*/
  protected int elementCountInRTree;

  /**Block size for the read file. Used with RTrees*/
  protected long blockSize;

  public SpatialRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    this(conf, split.getStartOffsets()[index], split.getLength(index),
        split.getPath(index));
  }
  
  public SpatialRecordReader(Configuration job, FileSplit split) throws IOException {
    this(job, split.getStart(), split.getLength(), split.getPath());
  }

  public SpatialRecordReader(Configuration job, long s, long l, Path p) throws IOException {
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
        Integer.MAX_VALUE);
    start = s;
    end = start + l;
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(p);

    // open the file and seek to the start of the split
    FileSystem fs = p.getFileSystem(job);
    in = fs.open(p);
    signature = new byte[8];

    InputStream is = in;
    if (codec != null) {
      // Compressed
      is = codec.createInputStream(in);
      end = Long.MAX_VALUE;
    } else {
      in.seek(start);
    }

    is.read(signature);
    isRTree = Arrays.equals(signature, SpatialSite.RTreeFileMarkerB);
    LOG.info("isRTree: "+isRTree+" at position "+start);
    if (isRTree) {
      // Block size is crucial for reading RTrees
      blockSize = fs.getFileStatus(p).getBlockSize();
      LOG.info("RTree block size "+blockSize);
      // File is an RTree
      if (!(is instanceof FSDataInputStream)) {
        in = new FSDataInputStream(is);
      }
    } else {
      boolean skipFirstLine = false;
      if (start != 0) {
        skipFirstLine = true;
      }
      // File is text file
      lineReader = new LineReader(is);
      tempLine = new Text();
      
      if (skipFirstLine) {
        boolean no_new_line_in_signature = true;
        for (int i = 0; i < signature.length; i++) {
          if (signature[i] == '\n' || signature[i] == '\r') {
            // Skip i bytes only
            byte[] tmp = new byte[signature.length - i - 1];
            System.arraycopy(signature, i+1, tmp, 0, tmp.length);
            signature = tmp;
            no_new_line_in_signature = false;
            start += i + 1;
            break;
          }
        }
        if (no_new_line_in_signature) {
          start += signature.length;
          // Didn't find the new line. Skip it from the reader
          start += lineReader.readLine(tempLine, 1000, (int)(end - start));
          signature = null;
        }
      }
    }
    this.pos = start;
  }
  
  public SpatialRecordReader(InputStream is, long offset, long endOffset) throws IOException {
    start = offset;
    end = endOffset;
    
    signature = new byte[8];
    is.read(signature);
    isRTree = Arrays.equals(signature, SpatialSite.RTreeFileMarkerB);
    LOG.info("isRTree: "+isRTree+" at "+start);
    
    if (isRTree) {
      blockSize = FileSystem.get(new Configuration()).getDefaultBlockSize();
      LOG.info("RTree size info guessed to "+blockSize);
      if (!(is instanceof FSDataInputStream)) {
        in = new FSDataInputStream(is);
      } else {
        in = (FSDataInputStream) is;
      }
    } else {
      boolean skipFirstLine = false;
      if (start != 0) {
        skipFirstLine = true;
      }
      // File is text file
      lineReader = new LineReader(is);
      tempLine = new Text();
      
      if (skipFirstLine) {
        boolean no_new_line_in_signature = true;
        for (int i = 0; i < signature.length; i++) {
          if (signature[i] == '\n' || signature[i] == '\r') {
            // Skip i bytes only
            byte[] tmp = new byte[signature.length - i - 1];
            System.arraycopy(signature, i+1, tmp, 0, tmp.length);
            signature = tmp;
            no_new_line_in_signature = false;
            start += i + 1;
            break;
          }
        }
        if (no_new_line_in_signature) {
          start += signature.length;
          // Didn't find the new line. Skip it from the reader
          start += lineReader.readLine(tempLine, 1000, (int)(end - start));
          signature = null;
        }
      }
    }
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
   * Reads the next line from input and return true if a line was read.
   * If no more lines are available in this split, a false is returned.
   * @param value
   * @return
   * @throws IOException
   */
  protected boolean nextLine(Text value) throws IOException {
    if (!isRTree) {
      if (getPos() > end)
        return false;
      int b = lineReader.readLine(value);
      pos += b;
      if (b == 0)
        return false;
      if (signature != null) {
        Text tmp = new Text();
        tmp.append(signature, 0, signature.length);
        tmp.append(value.getBytes(), 0, value.getLength());
        value.clear();
        value.append(tmp.getBytes(), 0, tmp.getLength());
        pos += signature.length;
        signature = null;
      }
      return value.getLength() != 0;
    } else {
      throw new RuntimeException("Not supported");
    }
  }
  
  /**
   * Reads next shape from input and returns true. If no more shapes are left
   * in the split, a false is returned.
   * @param s
   * @return
   * @throws IOException 
   */
  protected boolean nextShape(Shape s) throws IOException {
    if (isRTree) {
      if (elementCountInRTree == 0) {
        // Read next RTree
        if (signature == null) {
          // Check if there are more trees in file
          if (elementCountInRTree == 0) {
            if (pos >= end)
              return false;
            // Skip the rest of this block as only one RTree is stored per block
            in.seek(pos);
          }

          // Read and skip signature
          if (in.readLong() != SpatialSite.RTreeFileMarker) {
            throw new RuntimeException("RTree not found at: "+getPos());
          }
        }
        // Signature was already read in initialization.
        signature = null;
        elementCountInRTree = RTree.skipHeader(in);
        pos = in.getPos();
      }
      
      if (elementCountInRTree > 0) {
        s.readFields(in);
        elementCountInRTree--;
        if (elementCountInRTree == 0) {
          pos = in.getPos() + (blockSize - in.getPos() % blockSize)%blockSize;
        } else {
          pos = in.getPos();
        }
      } else {
        throw new RuntimeException();
      }
      return true;
    } else {
      if (!nextLine(tempLine))
        return false;
      s.fromText(tempLine);
      return true;
    }
  }
  
  /**
   * Reads all shapes left in this split in one shot.
   * @param shapes
   * @return
   * @throws IOException
   */
  protected boolean nextShapes(ArrayWritable shapes) throws IOException {
    if (isRTree) {
      try {
        // No more RTrees in file
        if (pos >= end)
          return false;
        // Check RTree signature
        if (signature == null) {
          in.seek(pos);
          // Read and skip signature
          if (in.readLong() != SpatialSite.RTreeFileMarker) {
            throw new RuntimeException("RTree not found at "+getPos());
          }
        }
        Shape stockObject = (Shape) shapes.getValueClass().newInstance();
        // Signature was already read in initialization.
        signature = null;
        int elementCount = RTree.skipHeader(in);
        Shape[] arshapes = new Shape[elementCount];
        while (elementCount-- > 0) {
          stockObject.readFields(in);
          arshapes[elementCount] = stockObject.clone();
        }
        // Skip the rest of this block as only one RTree is stored per block
        pos = in.getPos() + (blockSize - in.getPos() % blockSize)%blockSize;
        shapes.set(arshapes);
        return arshapes.length != 0;
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      return false;
    } else {
      try {
        Shape stockObject = (Shape) shapes.getValueClass().newInstance();
        Vector<Shape> vshapes = new Vector<Shape>();
        while (nextShape(stockObject)) {
          vshapes.add(stockObject.clone());
        }
        shapes.set(vshapes.toArray(new Shape[vshapes.size()]));
        return vshapes.size() != 0;
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      return false;
    }
  }
  
  /**
   * Reads the next RTree from file.
   * @param rtree
   * @return
   * @throws IOException
   */
  protected boolean nextRTree(RTree<? extends Shape> rtree) throws IOException {
    if (isRTree) {
      if (pos >= end)
        return false;
      if (signature == null) {
        in.seek(pos);
        // Read and skip signature
        if (in.readLong() != SpatialSite.RTreeFileMarker) {
          throw new RuntimeException("RTree not found");
        }
      }
      // Signature was already read in initialization.
      signature = null;
      rtree.readFields(in);
      // Skip the rest of this block as only one RTree is stored per block
      pos = in.getPos() + (blockSize - in.getPos() % blockSize)%blockSize;
      return true;
    } else {
      throw new RuntimeException("Not implemented");
    }
  }
  
  public static class ShapesRecordReader extends SpatialRecordReader<LongWritable, RTree> {
    ShapesRecordReader() throws IOException {
      super(FileSystem.getLocal(new Configuration()).open(new Path("file:///media/scratch/test.rtree")), 0, 134217728);
    }
    @Override
    public boolean next(LongWritable key, RTree value) throws IOException {
      key.set(getPos());
      return nextRTree(value);
    }
    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }
    @Override
    public RTree createValue() {
      RTree rtree = new RTree();
      rtree.setStockObject(new TigerShape());
      return rtree;
    }
  }
  
  public static void main(String[] args) throws IOException {
    ShapesRecordReader r = new ShapesRecordReader();
    LongWritable k = r.createKey();
    RTree v = r.createValue();
    int i = 0;
    while (r.next(k, v)) {
      i += v.getElementCount();
//      System.out.println(k+":"+v);
    }
    System.out.println("Read "+i+" items");
  }

}
