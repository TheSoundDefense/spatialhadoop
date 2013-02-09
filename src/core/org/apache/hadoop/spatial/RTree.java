package org.apache.hadoop.spatial;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;

/**
 * An RTree loaded in bulk and never changed after that. It cannot by
 * dynamically manipulated by either insertion or deletion. It only works with
 * 2-dimensional objects (keys).
 * @author eldawy
 *
 */
public class RTree<T extends Shape> implements Writable, Iterable<T> {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(RTree.class);
  
  /**Size of tree header on disk. Height + Degree + Number of records*/
  private static final int TreeHeaderSize = 4 + 4 + 4;

  /**Size of a node. Offset of first child + dimensions (x, y, width, height)*/
  private static final int NodeSize = 4 + 8 * 4;

  /** An instance of T that can be used to deserialize objects from disk */
  T stockObject;
  
  /** When reading the RTree from disk, this buffer contains the serialized
   * version of the tree as it appears on disk.
   */
  byte[] serializedTree;

  /**Input stream to tree data*/
  private DataInputStream dataIn;

  /**Height of the tree (number of levels)*/
  private int height;

  /**Degree of internal nodes in the tree*/
  private int degree;

  /**Total number of nodes in the tree*/
  private int nodeCount;

  /**Number of leaf nodes*/
  private int leafNodeCount;

  /**Number of non-leaf nodes*/
  private int nonLeafNodeCount;

  /**Number of elements in the tree*/
  private int elementCount;

  public RTree() {
  }

  /**
   *  Builds the RTree given a serialized list of elements. It uses the given
   * stockObject to deserialize these elements and build the tree.
   * Also writes the created tree to the disk directly.
   * @param elements - serialization of elements to be written
   * @param offset - index of the first element to use in the elements array
   * @param len - number of bytes to user from the elements array
   * @param degree - required degree of the tree to be built
   * @param dataOut - an output to use for writing the tree to
   * @param fast_sort - setting this to <code>true</code> allows the method
   *  to run faster by materializing the offset of each element in the list
   *  which speeds up the comparison. However, this requires an additional
   *  16 bytes per element. So, for each 1M elements, the method will require
   *  an additional 16 M bytes (approximately).
   */
  public void bulkLoadWrite(final byte[] element_bytes, final int offset, final int len,
      final int degree, DataOutput dataOut, final boolean fast_sort) {
    try {
    
      // Count number of elements in the given text
      int i_start = offset;
      final Text line = new Text();
      while (i_start < offset + len) {
        int i_end = skipToEOL(element_bytes, i_start);
        line.set(element_bytes, i_start, i_end - i_start);
        stockObject.fromText(line);
        elementCount++;
        i_start = i_end;
      }
      LOG.info("Bulk loading an RTree with "+elementCount+" elements");
      
      
      int height = Math.max(1, 
          (int) Math.ceil(Math.log(elementCount)/Math.log(degree)));
      int leafNodeCount = (int) Math.pow(degree, height - 1);
      if (elementCount <  2 * leafNodeCount && height > 1) {
        height--;
        leafNodeCount = (int) Math.pow(degree, height - 1);
      }
      int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
      int nonLeafNodeCount = nodeCount - leafNodeCount;

      // Keep track of the offset of each element in the text
      final int[] offsets = new int[elementCount];
      final long[] xs = fast_sort? new long[elementCount] : null;
      final long[] ys = fast_sort? new long[elementCount] : null;
      
      i_start = offset;
      line.clear();
      for (int i = 0; i < elementCount; i++) {
        offsets[i] = i_start;
        int i_end = skipToEOL(element_bytes, i_start);
        if (xs != null) {
          line.set(element_bytes, i_start, i_end - i_start);
          stockObject.fromText(line);
          xs[i] = stockObject.getMBR().getXMid();
          ys[i] = stockObject.getMBR().getYMid();
        }
        i_start = i_end;
      }

      /**A struct to store information about a split*/
      class SplitStruct extends Rectangle {
        /**Start and end index for this split*/
        int index1, index2;
        /**Direction of this split*/
        byte direction;
        /**Index of first element on disk*/
        int offsetOfFirstElement;
        
        static final byte DIRECTION_X = 0;
        static final byte DIRECTION_Y = 1;
        
        SplitStruct(int index1, int index2, byte direction) {
          this.index1 = index1;
          this.index2 = index2;
          this.direction = direction;
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
          out.writeInt(offsetOfFirstElement);
          super.write(out);
        }

        void partition(Queue<SplitStruct> toBePartitioned) {
          IndexedSortable sortableX;
          IndexedSortable sortableY;

          if (fast_sort) {
            // Use materialized xs[] and ys[] to do the comparisons
            sortableX = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap xs
                long tempx = xs[i];
                xs[i] = xs[j];
                xs[j] = tempx;
                // Swap ys
                long tempY = ys[i];
                ys[i] = ys[j];
                ys[j] = tempY;
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                if (xs[i] < xs[j])
                  return -1;
                if (xs[i] > xs[j])
                  return 1;
                return 0;
              }
            };
            
            sortableY = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap xs
                long tempx = xs[i];
                xs[i] = xs[j];
                xs[j] = tempx;
                // Swap ys
                long tempY = ys[i];
                ys[i] = ys[j];
                ys[j] = tempY;
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                if (ys[i] < ys[j])
                  return -1;
                if (ys[i] > ys[j])
                  return 1;
                return 0;
              }
            };
          } else {
            // No materialized xs and ys. Always deserialize objects to compare
            sortableX = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                // Get end of line
                int eol = skipToEOL(element_bytes, offsets[i]);
                line.set(element_bytes, offsets[i], eol - offsets[i]);
                stockObject.fromText(line);
                long xi = stockObject.getMBR().getXMid();

                eol = skipToEOL(element_bytes, offsets[j]);
                line.set(element_bytes, offsets[j], eol - offsets[j]);
                stockObject.fromText(line);
                long xj = stockObject.getMBR().getXMid();
                if (xi < xj)
                  return -1;
                if (xi > xj)
                  return 1;
                return 0;
              }
            };
            
            sortableY = new IndexedSortable() {
              @Override
              public void swap(int i, int j) {
                // Swap id
                int tempid = offsets[i];
                offsets[i] = offsets[j];
                offsets[j] = tempid;
              }
              
              @Override
              public int compare(int i, int j) {
                int eol = skipToEOL(element_bytes, offsets[i]);
                line.set(element_bytes, offsets[i], eol - offsets[i]);
                stockObject.fromText(line);
                long yi = stockObject.getMBR().getYMid();

                eol = skipToEOL(element_bytes, offsets[j]);
                line.set(element_bytes, offsets[j], eol - offsets[j]);
                stockObject.fromText(line);
                long yj = stockObject.getMBR().getYMid();
                if (yi < yj)
                  return -1;
                if (yi > yj)
                  return 1;
                return 0;
              }
            };
          }

          final IndexedSorter sorter = new QuickSort();
          
          final IndexedSortable[] sortables = new IndexedSortable[2];
          sortables[SplitStruct.DIRECTION_X] = sortableX;
          sortables[SplitStruct.DIRECTION_Y] = sortableY;
          
          sorter.sort(sortables[direction], index1, index2);

          // Partition into maxEntries partitions (equally) and
          // create a SplitStruct for each partition
          int i1 = index1;
          for (int iSplit = 0; iSplit < degree; iSplit++) {
            int i2 = index1 + (index2 - index1) * (iSplit + 1) / degree;
            SplitStruct newSplit = new SplitStruct(i1, i2, (byte)(1 - direction));
            toBePartitioned.add(newSplit);
            i1 = i2;
          }
        }
      }
      
      // All nodes stored in level-order traversal
      Vector<SplitStruct> nodes = new Vector<SplitStruct>();
      final Queue<SplitStruct> toBePartitioned = new LinkedList<SplitStruct>();
      toBePartitioned.add(new SplitStruct(0, elementCount, SplitStruct.DIRECTION_X));
      
      while (!toBePartitioned.isEmpty()) {
        SplitStruct split = toBePartitioned.poll();
        if (nodes.size() < nonLeafNodeCount) {
          // This is a non-leaf
          split.partition(toBePartitioned);
        }
        nodes.add(split);
      }
      
      if (nodes.size() != nodeCount) {
        throw new RuntimeException("Expected node count: "+nodeCount+". Real node count: "+nodes.size());
      }
      
      // Now we have our data sorted in the required order. Start building
      // the tree.
      // Store the offset of each leaf node in the tree
      FSDataOutputStream fakeOut =
          new FSDataOutputStream(new java.io.OutputStream() {
            // Null output stream
            @Override
            public void write(int b) throws IOException {
              // Do nothing
            }
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
              // Do nothing
            }
            @Override
            public void write(byte[] b) throws IOException {
              // Do nothing
            }
          }, null, TreeHeaderSize + nodes.size() * NodeSize);
      for (int i_leaf = nonLeafNodeCount, i=0; i_leaf < nodes.size(); i_leaf++) {
        nodes.elementAt(i_leaf).offsetOfFirstElement = (int)fakeOut.getPos();
        if (i != nodes.elementAt(i_leaf).index1) throw new RuntimeException();
        long x1 = Long.MAX_VALUE;
        long y1 = Long.MAX_VALUE;
        long x2 = Long.MIN_VALUE;
        long y2 = Long.MIN_VALUE;
        while (i < nodes.elementAt(i_leaf).index2) {
          int eol = skipToEOL(element_bytes, offsets[i]);
          fakeOut.write(element_bytes, offsets[i],
              eol - offsets[i]);
          line.set(element_bytes, offsets[i], eol - offsets[i]);
          stockObject.fromText(line);
          Rectangle mbr = stockObject.getMBR();
          if (mbr.getX1() < x1) x1 = mbr.getX1();
          if (mbr.getY1() < y1) y1 = mbr.getY1();
          if (mbr.getX2() > x2) x2 = mbr.getX2();
          if (mbr.getY2() > y2) y2 = mbr.getY2();
          i++;
        }
        nodes.elementAt(i_leaf).set(x1, y1, x2-x1, y2-y1);
      }
      fakeOut.close(); fakeOut = null;
      
      // Calculate MBR and offsetOfFirstElement for non-leaves
      for (int i_node = nonLeafNodeCount-1; i_node >= 0; i_node--) {
        int i_first_child = i_node * degree + 1;
        nodes.elementAt(i_node).offsetOfFirstElement =
            nodes.elementAt(i_first_child).offsetOfFirstElement;
        long x1 = Long.MAX_VALUE;
        long y1 = Long.MAX_VALUE;
        long x2 = Long.MIN_VALUE;
        long y2 = Long.MIN_VALUE;
        for (int i_child = 0; i_child < degree; i_child++) {
          Rectangle mbr = nodes.elementAt(i_first_child + i_child);
          if (mbr.getX1() < x1) x1 = mbr.getX1();
          if (mbr.getY1() < y1) y1 = mbr.getY1();
          if (mbr.getX2() > x2) x2 = mbr.getX2();
          if (mbr.getY2() > y2) y2 = mbr.getY2();
        }
        nodes.elementAt(i_node).set(x1, y1, x2-x1, y2-y1);
      }
      
      // Start writing the tree
      // write tree header (including size)
      // Total tree size. (== Total bytes written - 8 bytes for the size itself)
      dataOut.writeInt(TreeHeaderSize + NodeSize * nodeCount + len);
      // Tree height
      dataOut.writeInt(height);
      // Degree
      dataOut.writeInt(degree);
      dataOut.writeInt(elementCount);
      
      // write nodes
      for (SplitStruct node : nodes) {
        node.write(dataOut);
      }
      // write elements
      for (int element_i = 0; element_i < elementCount; element_i++) {
        int eol = skipToEOL(element_bytes, offsets[element_i]);
        dataOut.write(element_bytes, offsets[element_i],
            eol - offsets[element_i]);
      }
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    throw new RuntimeException("write is no longer supported. " +
    		"Please use bulkLoadWrite to write the RTree.");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int treeSize = in.readInt();
    if (treeSize == 0) {
      serializedTree = null;
    } else {
      serializedTree = new byte[treeSize];
      in.readFully(serializedTree);
    }
    readHeader();
  }
  
  /**
   * Reads and skips the header of the tree returning the total number of
   * bytes skipped from the stream. This is used as a preparatory function to
   * read all elements in the tree without the index part.
   * @param in
   * @return - Total number of bytes read and skipped
   * @throws IOException
   */
  public static int skipHeader(InputStream in) throws IOException {
    DataInput dataIn = in instanceof DataInput ? (DataInput) in
        : new DataInputStream(in);
    int skippedBytes = 0;
    /*int treeSize = */dataIn.readInt(); skippedBytes += 4;
    int height = dataIn.readInt(); skippedBytes += 4;
    if (height == 0) {
      // Empty tree. No results
      return skippedBytes;
    }
    int degree = dataIn.readInt(); skippedBytes += 4;
    int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    /*int elementCount = */dataIn.readInt(); skippedBytes += 4;
    // Skip all nodes
    dataIn.skipBytes(nodeCount * NodeSize); skippedBytes += nodeCount * NodeSize;
    return skippedBytes;
  }
  
  /**
   * Returns the total size of the header (including the index) in bytes.
   * Assume that the input is aligned to the start offset of the tree (header).
   * Note that the part of the header is consumed from the given input to be
   * able to determine header size.
   * @param in
   * @return
   * @throws IOException
   */
  public static int getHeaderSize(DataInput in) throws IOException {
    int header_size = 0;
    /*int treeSize = */in.readInt(); header_size += 4;
    int height = in.readInt(); header_size += 4;
    if (height == 0) {
      // Empty tree. No results
      return header_size;
    }
    int degree = in.readInt(); header_size += 4;
    int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    /*int elementCount = */in.readInt(); header_size += 4;
    // Add the size of all nodes
    header_size += nodeCount * NodeSize;
    return header_size;
  }

  private void readHeader() throws IOException {
    startQuery();
    endQuery();
  }

  /**
   * Returns total number of elements
   * @return
   */
  public int getElementCount() {
    return elementCount;
  }
  
  /**
   * Returns the MBR of the root
   * @return
   */
  public Rectangle getMBR() {
    Rectangle mbr = null;
    try {
      startQuery();
      dataIn.reset(); dataIn.skip(TreeHeaderSize);
      mbr = new Rectangle();
      /*int offset = */dataIn.readInt();
      mbr.readFields(dataIn);
      endQuery();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return mbr;
  }
  
  /**
   * Reads and returns the element with the given index
   * @param i
   * @return
   * @throws IOException 
   */
  public T readElement(int i) {
    Iterator<T> iter = iterator();
    while (i-- > 0 && iter.hasNext()) {
      iter.next();
    }
    return iter.next();
  }

  public void setStockObject(T stockObject) {
    this.stockObject = stockObject;
  }
  
  /**
   * Create rectangles that together pack all points in sample such that
   * each rectangle contains roughly the same number of points. In other words
   * it tries to balance number of points in each rectangle.
   * Works similar to the logic of bulkLoad but does only one level of
   * rectangles.
   * @param samples
   * @param gridInfo - Used as a hint for number of rectangles per row or column
   * @return
   */
  public static Rectangle[] packInRectangles(GridInfo gridInfo, final Point[] sample) {
    Rectangle[] rectangles = new Rectangle[gridInfo.columns * gridInfo.rows];
    int iRectangle = 0;
    // Sort in x direction
    final IndexedSortable sortableX = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Point temp = sample[i];
        sample[i] = sample[j];
        sample[j] = temp;
      }

      @Override
      public int compare(int i, int j) {
        if (sample[i].x < sample[j].x)
          return -1;
        if (sample[i].x > sample[j].x)
          return 1;
        return 0;
      }
    };

    // Sort in y direction
    final IndexedSortable sortableY = new IndexedSortable() {
      @Override
      public void swap(int i, int j) {
        Point temp = sample[i];
        sample[i] = sample[j];
        sample[j] = temp;
      }

      @Override
      public int compare(int i, int j) {
        if (sample[i].y < sample[j].y)
          return -1;
        if (sample[i].y > sample[j].y)
          return 1;
        return 0;
      }
    };

    final QuickSort quickSort = new QuickSort();
    
    quickSort.sort(sortableX, 0, sample.length);

    int xindex1 = 0;
    long x1 = gridInfo.xOrigin;
    for (int col = 0; col < gridInfo.columns; col++) {
      int xindex2 = sample.length * (col + 1) / gridInfo.columns;
      
      // Determine extents for all rectangles in this column
      long x2 = col == gridInfo.columns - 1 ? 
          gridInfo.xOrigin + gridInfo.gridWidth : sample[xindex2-1].x;
      
      // Sort all points in this column according to its y-coordinate
      quickSort.sort(sortableY, xindex1, xindex2);
      
      // Create rectangles in this column
      long y1 = gridInfo.yOrigin;
      for (int row = 0; row < gridInfo.rows; row++) {
        int yindex2 = xindex1 + (xindex2 - xindex1) *
            (row + 1) / gridInfo.rows;
        long y2 = row == gridInfo.rows - 1 ? 
            gridInfo.yOrigin + gridInfo.gridHeight : sample[yindex2-1].y;
        
        rectangles[iRectangle++] = new Rectangle(x1, y1, x2-x1, y2-y1);
        y1 = y2;
      }
      
      xindex1 = xindex2;
      x1 = x2;
    }
    return rectangles;
  }
  
  /**
   * An iterator that goes over all elements in the tree in no particular order
   * @author eldawy
   *
   */
  class RTreeIterator implements Iterator<T> {

    /**Current offset in the array of elements*/
    int offset;
    
    /**Temporary text that holds one line to deserialize objects*/
    Text line;
    
    /**A stock object to read from stream*/
    T _stockObject;
    
    RTreeIterator() throws IOException {
      if (RTree.this.height == 0) {
        offset = RTree.this.serializedTree.length;
        return;
      }
      offset = TreeHeaderSize + NodeSize * RTree.this.nodeCount;
      _stockObject = (T) RTree.this.stockObject.clone();
      line = new Text();
    }

    @Override
    public boolean hasNext() {
      return offset < RTree.this.serializedTree.length;
    }

    @Override
    public T next() {
      // get the end of current line and deserialize stock object
      int eol = skipToEOL(serializedTree, offset);
      line.set(serializedTree, offset, eol - offset);
      _stockObject.fromText(line);
      offset = eol; // Advance to next item
      return _stockObject;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not supported");
    }
  }
  
  /**
   * Skip bytes until the end of line
   * @param bytes
   * @param startOffset
   * @return
   */
  public static int skipToEOL(byte[] bytes, int startOffset) {
    int eol = startOffset;
    while (eol < bytes.length && (bytes[eol] != '\n' && bytes[eol] != '\r'))
      eol++;
    while (eol < bytes.length && (bytes[eol] == '\n' || bytes[eol] == '\r'))
      eol++;
    return eol;
  }

  @Override
  public Iterator<T> iterator() {
    try {
      return new RTreeIterator();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Given a block size, record size and a required tree degree, this function
   * calculates the maximum number of records that can be stored in this
   * block taking into consideration the overhead needed by node structure.
   * @param blockSize
   * @param degree
   * @param recordSize
   * @return
   */
  public static int getBlockCapacity(long blockSize, int degree, int recordSize) {
    double a = (double)NodeSize / (degree - 1);
    double ratio = (blockSize + a) / (recordSize + a);
    double break_even_height = Math.log(ratio) / Math.log(degree);
    double h_min = Math.floor(break_even_height);
    double capacity1 = Math.floor(Math.pow(degree, h_min));
    double structure_size = 4 + TreeHeaderSize + a * (capacity1 * degree - 1);
    double capacity2 = Math.floor((blockSize - structure_size) / recordSize);
    return Math.max((int)capacity1, (int)capacity2);
  }
  
  /**
   * Prepares a tree for running a query. This method creates an input stream
   * that reads from the stored buffer.
   * @throws IOException 
   */
  protected void startQuery() throws IOException {
    dataIn = new DataInputStream(new ByteArrayInputStream(serializedTree));
    height = dataIn.readInt();
    if (height == 0) {
      // Empty tree. No results
      return;
    }
    degree = dataIn.readInt();
    nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    leafNodeCount = (int) Math.pow(degree, height - 1);
    nonLeafNodeCount = nodeCount - leafNodeCount;
    elementCount = dataIn.readInt();
  }
  
  /**
   * This method is called after the query ends to close the in-memory input
   * stream opened
   * @throws IOException
   */
  protected void endQuery() throws IOException {
    dataIn.close();
    dataIn = null;
  }
  
  /**
   * Searches the RTree starting from the given start position. This is either
   * a node number or offset of an element. If it's a node number, it performs
   * the search in the subtree rooted at this node. If it's an offset number,
   * it searches only the object found there.
   * It is assumed that the openQuery() has been called before this function
   * and that endQuery() will be called afterwards.
   * @param query_mbr
   * @param output
   * @param start - where to start searching
   * @param end - where to end searching. Only used when start is an offset of
   *   an object.
   * @return
   * @throws IOException 
   */
  protected int search(Shape query_shape, ResultCollector<T> output, int start,
      int end)
      throws IOException {
    Rectangle query_mbr = query_shape.getMBR();
    int resultSize = 0;
    // Special case for an empty tree
    if (height == 0)
      return 0;

    Stack<Integer> toBeSearched = new Stack<Integer>();
    // Start from the given node
    toBeSearched.push(start);
    if (start >= nodeCount) {
      toBeSearched.push(end);
    }

    Rectangle node_mbr = new Rectangle();
    
    Text line = new Text();

    while (!toBeSearched.isEmpty()) {
      int searchNumber = toBeSearched.pop();
      int mbrsToTest = searchNumber == 0 ? 1 : degree;

      if (searchNumber < nodeCount) {
        long nodeOffset = TreeHeaderSize + NodeSize * searchNumber;
        dataIn.reset(); dataIn.skip(nodeOffset);
        int dataOffset = dataIn.readInt();

        for (int i = 0; i < mbrsToTest; i++) {
          node_mbr.readFields(dataIn);
          int lastOffset = (searchNumber+i) == nodeCount - 1 ?
              serializedTree.length : dataIn.readInt();
          if (query_mbr.contains(node_mbr)) {
            // The node is full contained in the query range.
            // Save the time and do full scan for this node
            toBeSearched.push(dataOffset);
            // Checks if this node is the last node in its level
            // This can be easily detected because the next node in the level
            // order traversal will be the first node in the next level
            // which means it will have an offset less than this node
            if (lastOffset <= dataOffset)
              lastOffset = serializedTree.length;
            toBeSearched.push(lastOffset);
          } else if (query_mbr.isIntersected(node_mbr)) {
            // Node partially overlaps with query. Go deep under this node
            if (searchNumber < nonLeafNodeCount) {
              // Search child nodes
              toBeSearched.push((searchNumber + i) * degree + 1);
            } else {
              // Search all elements in this node
              toBeSearched.push(dataOffset);
              // Checks if this node is the last node in its level
              // This can be easily detected because the next node in the level
              // order traversal will be the first node in the next level
              // which means it will have an offset less than this node
              if (lastOffset <= dataOffset)
                lastOffset = serializedTree.length;
              toBeSearched.push(lastOffset);
            }
          }
          dataOffset = lastOffset;
        }
      } else {
        int firstOffset, lastOffset;
        // Search for data items (records)
        lastOffset = searchNumber;
        firstOffset = toBeSearched.pop();
        
        // while (bytes read so far < total bytes that need to be read)
        while (firstOffset < lastOffset) {
          // Read one line
          int eol = skipToEOL(serializedTree, firstOffset);
          line.set(serializedTree, firstOffset, eol - firstOffset);

          stockObject.fromText(line);
          if (stockObject.isIntersected(query_shape)) {
            resultSize++;
            if (output != null)
              output.collect(stockObject);
          }
          firstOffset = eol;
        }
      }
    }
    return resultSize;
  }
  
  /**
   * Performs a range query over this tree using the given query range.
   * @param query - The query rectangle to use (TODO make it any shape not just rectangle)
   * @param output - Shapes found are reported to this output. If null, results are not reported
   * @return - Total number of records found
   */
  public int search(Shape query, ResultCollector<T> output) {
    // Check for an empty tree
    if (serializedTree == null)
      return 0;
    
    int resultCount = 0;
    
    try {
      startQuery();
      resultCount = search(query, output, 0, 0);
      endQuery();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return resultCount;
  }
  
  /**
   * k nearest neighbor query
   * Note: Current algorithm is approximate just for simplicity. Writing an exact
   * algorithm is on our TODO list
   * @param qx
   * @param qy
   * @param k
   * @param output
   */
  public int knn(final long qx, final long qy, int k, final ResultCollector2<T, Long> output) {
    double query_area = ((double) getMBR().width * getMBR().height) * k / getElementCount();
    double query_radius = Math.sqrt(query_area / Math.PI);

    boolean result_correct;
    final Vector<Long> distances = new Vector<Long>();
    final Vector<T> shapes = new Vector<T>();
    // Find results in the range and increase this range if needed to ensure
    // correctness of the answer
    do {
      // Initialize result and query range
      distances.clear(); shapes.clear();
      Rectangle queryRange = new Rectangle();
      queryRange.width = (long) (2 * query_radius);
      queryRange.height = (long) (2 * query_radius);
      queryRange.x = qx - queryRange.width / 2;
      queryRange.y = qy - queryRange.height / 2;
      // Retrieve all results in range
      search(queryRange, new ResultCollector<T>() {
        @Override
        public void collect(T shape) {
          distances.add((long) shape.distanceTo(qx, qy));
          shapes.add(shape);
        }
      });
      if (shapes.size() < k) {
        // Didn't find k elements in range, double the range to get more items
        if (shapes.size() == getElementCount()) {
          // Already returned all possible elements
          result_correct = true;
        } else {
          query_radius *= 2;
          result_correct = false;
        }
      } else {
        // Sort items by distance to get the kth neighbor
        IndexedSortable s = new IndexedSortable() {
          @Override
          public void swap(int i, int j) {
            long temp_distance = distances.elementAt(i);
            distances.set(i, distances.elementAt(j));
            distances.set(j, temp_distance);
            
            T temp_shape = shapes.elementAt(i);
            shapes.set(i, shapes.elementAt(j));
            shapes.set(j, temp_shape);
          }
          @Override
          public int compare(int i, int j) {
            // Note. Equality is not important to check because items with the
            // same distance can be ordered anyway. 
            if (distances.elementAt(i) < distances.elementAt(j))
              return -1;
            return 1;
          }
        };
        IndexedSorter sorter = new QuickSort();
        sorter.sort(s, 0, shapes.size());
        if (distances.elementAt(k - 1) > query_radius) {
          result_correct = false;
          query_radius = distances.elementAt(k);
        } else {
          result_correct = true;
        }
      }
    } while (!result_correct);
    
    int result_size = Math.min(k,  shapes.size());
    if (output != null) {
      for (int i = 0; i < result_size; i++) {
        output.collect(shapes.elementAt(i), distances.elementAt(i));
      }
    }
    return result_size;
  }

  public static<S1 extends Shape, S2 extends Shape> int spatialJoin(
      final RTree<S1> R,
      final RTree<S2> S,
      final ResultCollector2<S1, S2> output)
      throws IOException {
    S1[] rs = (S1[]) Array.newInstance(R.stockObject.getClass(), R.getElementCount());
    int i = 0;
    for (S1 r : R) {
      rs[i++] = r;
    }
    S2[] ss = (S2[]) Array.newInstance(S.stockObject.getClass(), S.getElementCount());
    i = 0;
    for (S2 s : S) {
      ss[i++] = s;
    }
    return SpatialAlgorithms.SpatialJoin_planeSweep(rs, ss, output);
  }
  
  /**
   * Calculate the storage overhead required to build an RTree for the given
   * number of nodes.
   * @return - storage overhead in bytes
   */
  public static int calculateStorageOverhead(int elementCount, int degree){
    // Update storage overhead
    int height = Math.max(1, 
        (int) Math.ceil(Math.log(elementCount)/Math.log(degree)));
    int leafNodeCount = (int) Math.pow(degree, height - 1);
    if (elementCount <  2 * leafNodeCount && height > 1) {
      height--;
      leafNodeCount = (int) Math.pow(degree, height - 1);
    }
    int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
    int storage_overhead = 4 + TreeHeaderSize + nodeCount * NodeSize;
    return storage_overhead;
  }
  
}
