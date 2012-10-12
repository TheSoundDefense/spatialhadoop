package org.apache.hadoop.spatial;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

/**
 * An RTree loaded in bulk and never changed after that. It cannot by
 * dynamically manipulated by either insertion or deletion. It only works with
 * 2-dimensional objects (keys).
 * @author eldawy
 *
 */
public class RTree<T extends Shape> implements Writable {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(RTree.class);
  
  /**
   * A node in the RTree
   * @author eldawy
   *
   */
  static class Node extends Rectangle {
    // Children of each node could be either other nodes or elements
    Vector<Shape> children;
    // When we read elements from disk and we want to delay parsing values,
    // we use element data to store the raw data of all children.
    // Only when this node is queried, we parse these values and store them
    // in children list
    byte[] elementData;
    boolean leaf;
    
    Node(boolean leaf) {
      this.leaf = leaf;
    }
    
    public int getNodeHeight() {
      if (leaf)
        return 1;
      int height = 0;
      for (int i = 0; i < children.size(); i++)
        height = Math.max(height, ((Node)children.elementAt(0)).getNodeHeight());
      return 1 + height;
    }

    public int getNodeCount() {
      if (leaf)
        return 1;
      int totalSize = 1;
      for (int i = 0; i < children.size(); i++) {
        totalSize += ((Node)children.elementAt(0)).getNodeCount();
      }
      return totalSize;
    }

    public int getElementCount() {
      if (leaf)
        return children.size();
      int totalSize = 0;
      for (int i = 0; i < children.size(); i++) {
        totalSize += ((Node)children.elementAt(0)).getElementCount();
      }
      return totalSize;
    }
    
    public Rectangle recalculateMBR() {
      Shape allArea;
      if (leaf) {
        allArea = children.elementAt(0);
        for (int i = 1; i < children.size(); i++) {
          allArea = allArea.union(children.elementAt(i));
        }
      } else {
        allArea = ((Node)children.elementAt(0)).recalculateMBR();
        for (int i = 1; i < children.size(); i++) {
          allArea = allArea.union(((Node)children.elementAt(i)).recalculateMBR());
        }
      }
      Rectangle mbr = allArea.getMBR();
      this.set(mbr.x, mbr.y, mbr.width, mbr.height);
      return this;
    }
  }

  /**Size of tree header on disk. Height + Degree + Number of records*/
  private static final int TreeHeaderSize = 4 + 4 + 4;

  /**Size of a node. Offset of first child + dimensions (x, y, width, height)*/
  private static final int NodeSize = 4 + 8 * 4;

  /// The root node for the tree. null if the tree is empty.
  Node root;
  
  // An instance of T that can be used to deserialize objects from disk
  T stockObject;
  
  // When reading the RTree from disk, this buffer contains the serialized
  // version of the tree as it appears on disk.
  byte[] serializedTree;

  public RTree() {
  }

  /**
   * Builds the RTree by loading all given objects. The maximum fan-out of any
   * node in the tree should be maxEntries. We try as much as we can to use
   * this maxEntries and balance the tree.
   * @param elements
   * @param maxEntries
   */
  public void bulkLoad(final T[] elements, final int maxEntries) {
    class SplitStruct {
      // The root node that will contain data to be split
      Node node;
      // The first index in elements
      int index1;
      // The last index in elements
      int index2;
      // The direction in which we should split children
      int direction;
//      static final int DIRECTION_X = 0;
//      static final int DIRECTION_Y = 1;
      
      /**
       * Creates a split struct which saves the state at some point. This split
       * needs to be partitioned later.
       * @param nodeParent
       * @param index1
       * @param index2
       * @param direction - the direction in which we should split leaves
       *   0: x direction
       *   1: y direction
       */
      SplitStruct(Node nodeParent, int index1, int index2, int direction) {
        this.index1 = index1;
        this.index2 = index2;
        this.direction = direction;

        if (index2 - index1 <= maxEntries) {
          // All the elements in this partition can fit in one (leaf) node
          node = new Node(true);
          node.children = new Vector<Shape>();
          while (index1 < index2) {
            node.children.add(elements[index1]);
            index1++;
          }
        } else {
          node = new Node(false);
          node.children = new Vector<Shape>();
        }

        // Add as a child to parent node or set to root if no parent node
        if (nodeParent == null) {
          RTree.this.root = node;
        } else {
          nodeParent.children.add(node);
        }
      }

      void partition(Stack<SplitStruct> toBePartitioned) {
        final IndexedSortable sortableX = new IndexedSortable() {
          @Override
          public void swap(int i, int j) {
            T temp = elements[i];
            elements[i] = elements[j];
            elements[j] = temp;
          }

          @Override
          public int compare(int i, int j) {
            return (int) (elements[i].getMBR().x - elements[j].getMBR().x);
          }
        };

        final IndexedSortable sortableY = new IndexedSortable() {
          @Override
          public void swap(int i, int j) {
            T temp = elements[i];
            elements[i] = elements[j];
            elements[j] = temp;
          }

          @Override
          public int compare(int i, int j) {
            return (int) (elements[i].getMBR().y - elements[j].getMBR().y);
          }
        };

        final QuickSort quickSort = new QuickSort();
        
        final IndexedSortable[] sortables = new IndexedSortable[] {
            sortableX, sortableY
        };
        
        quickSort.sort(sortables[direction], index1, index2);

        // Partition into maxEntries partitions (equally) and
        // create a SplitStruct for each partition
        int i1 = index1;
        for (int iSplit = 0; iSplit < maxEntries; iSplit++) {
          int i2 = index1 + (index2 - index1) * (iSplit + 1) / maxEntries;
          SplitStruct newSplit = new SplitStruct(node, i1, i2, 1 - direction);
          toBePartitioned.push(newSplit);
          i1 = i2;
        }
      }
    }
    
    if (elements.length == 0) {
      this.root = null;
      return;
    }
    
    final Stack<SplitStruct> toBePartitioned = new Stack<SplitStruct>();
    toBePartitioned.push(new SplitStruct(null, 0, elements.length, 0));
    
    while (!toBePartitioned.isEmpty()) {
      SplitStruct split = toBePartitioned.pop();
      // No need to partition leaves
      if (split.node.leaf)
        continue;
      
      // Partition this node according to its direction
      split.partition(toBePartitioned);
    }
    
    this.root.recalculateMBR();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    FSDataOutputStream tempDataOut = new FSDataOutputStream(
        new NullOutputStream(), null);
    
    // A list of nodes to be written do disk. Nodes are written in their
    // level-order traversal
    Queue<Node> toBeWritten = new LinkedList<Node>();
    long offsetOfFirstElement = 0;
    
    int nodeCount;
    int height;
    int degree;
    if (root != null) {
      LOG.info("Writing an RTree with root: "+root.getMBR());
      height = getTreeHeight();
      degree = root.children.size();
      nodeCount = degree == 1 && height == 1 ?
          1 : (int) ((Math.pow(degree, height) - 1) / (degree - 1));
      if (nodeCount != getNodeCount())
        throw new RuntimeException("Node count "+nodeCount+" != "+getNodeCount());
      
      offsetOfFirstElement = TreeHeaderSize +
          NodeSize * nodeCount;
      
      // Start writing elements from the root
      toBeWritten.add(root);
    } else {
      height = 0;
      nodeCount = 0;
      degree = 0;
      offsetOfFirstElement = 4;
    }

    // Stores non leaves in level order traversal to be able 
    Vector<Node> nonLeavesInLevelOrderTraversal = new Vector<Node>();
    // Map each node to the offset of first child on disk.
    // For leaves, the first child is a direct child to the node
    // For non leaves, this offset is equal to the offset of first child for
    // its left most child node
    Map<Node, Integer> offsetOfFirstChild = new HashMap<Node, Integer>();
    while (!toBeWritten.isEmpty()) {
      // Get next element to be visited
      Node n = toBeWritten.poll();
      
      if (!n.leaf) {
        nonLeavesInLevelOrderTraversal.add(n);
        // Serialize all child nodes
        for (Shape child : n.children) {
          toBeWritten.add((Node)child);
        }
      } else {
        // For leaves. Record the offset of the first element in this node
        offsetOfFirstChild.put(n,
            (int) (offsetOfFirstElement + tempDataOut.getPos()));
        // Then, write elements to data out
        for (Shape s : n.children) {
          s.write(tempDataOut);
        }
      }
    }
    // No longer need this temp data out
    tempDataOut.close();
    
    // Calculate offset of first child for all nodes
    for (int i = nonLeavesInLevelOrderTraversal.size() - 1; i >= 0; i--) {
      Node n = nonLeavesInLevelOrderTraversal.elementAt(i);
      offsetOfFirstChild.put(n, offsetOfFirstChild.get(n.children.firstElement()));
    }
    nonLeavesInLevelOrderTraversal = null;

    if (root != null)
      toBeWritten.add(root);
    
    int treeSize = (int) (offsetOfFirstElement + tempDataOut.getPos());

    // Actually write data to output
    // Write tree header
    out.writeInt(treeSize);
    out.writeInt(height);
    out.writeInt(degree);
    out.writeInt(getElementCount());
    
    Vector<Node> leaves = new Vector<Node>();
    while (!toBeWritten.isEmpty()) {
      Node n = toBeWritten.poll();
      out.writeInt(offsetOfFirstChild.get(n));
      n.write(out);
      if (!n.leaf) {
        // Serialize all child nodes
        for (Shape child : n.children) {
          toBeWritten.add((Node)child);
        }
      } else {
        leaves.add(n);
      }
    }
    for (Node leaf : leaves) {
      // Write elements to data out
      for (Shape s : leaf.children) {
        s.write(out);
      }
    }
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
    root = null;
  }

  public int getNodeCount() {
    return root == null? 0 : root.getNodeCount();
  }
  
  public int getElementCount() {
    return root == null? 0 : root.getElementCount();
  }
  
  public int getTreeHeight() {
    return root == null? 0 : root.getNodeHeight();
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
        return (int) (sample[i].x - sample[j].x);
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
        return (int) (sample[i].y - sample[j].y);
      }
    };

    final QuickSort quickSort = new QuickSort();
    
    quickSort.sort(sortableX, 0, sample.length);

    int xindex1 = 0;
    long x1 = gridInfo.xOrigin;
    for (int col = 0; col < gridInfo.columns; col++) {
      int xindex2 = sample.length * (col + 1) / gridInfo.rows;
      
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
   * Used to collect all results from a range or point query.
   * 
   * @author eldawy
   * 
   * @param <T>
   */
  public static interface ResultCollector<T> {
    void add(T x);
  }
  
  public int search(Rectangle query, ResultCollector<T> results) {
    return root != null?
        searchMemory(query, results) : searchDisk(query, results);
  }

  /**
   * Searches for objects in the query rectangle and outputs them to the
   * given result collector. The function returns total number of items found
   * by this search.
   * @param query
   * @param object
   * @return
   */
  protected int searchMemory(Rectangle query, ResultCollector<T> results) {
    int resultSize = 0;
    Stack<Node> toBeSearched = new Stack<Node>();
    toBeSearched.push(root);
    
    while (!toBeSearched.isEmpty()) {
      Node p = toBeSearched.pop();
      if (p.leaf) {
        for (Shape s : p.children) {
          if (s.isIntersected(query)) {
            resultSize++;
            if (results != null)
              results.add((T)s);
          }
        }
      } else {
        for (Shape n : p.children) {
          if (n.isIntersected(query)) {
            toBeSearched.push((Node)n);
          }
        }
      }
    }
    return resultSize;
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
  
  protected int searchDisk(Rectangle query, ResultCollector<T> output) {
    int resultSize = 0;

    // Check for an empty tree
    if (serializedTree == null)
      return 0;
    try {
      DataInputStream in =
          new DataInputStream(new ByteArrayInputStream(serializedTree));
      int height = in.readInt();
      if (height == 0) {
        // Empty tree. No results
        return resultSize;
      }
      int degree = in.readInt();
      int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
      int leafNodeCount = (int) Math.pow(degree, height - 1);
      int nonLeafNodeCount = nodeCount - leafNodeCount;
      /*int elementCount =*/ in.readInt();
      
      in.reset();
      in.skip(TreeHeaderSize);

      Queue<Integer> toBeSearched = new LinkedList<Integer>();
      toBeSearched.add(0);

      Rectangle mbr = new Rectangle();

      while (!toBeSearched.isEmpty()) {
        int searchNumber = toBeSearched.poll();
        int mbrsToTest = searchNumber == 0 ? 1 : degree;

        if (searchNumber < nodeCount) {
          long nodeOffset = TreeHeaderSize + NodeSize * searchNumber;
          in.reset(); in.skip(nodeOffset);
          int dataOffset = in.readInt();
          
          for (int i = 0; i < mbrsToTest; i++) {
            mbr.readFields(in);
            LOG.info("Comparing with the node: "+mbr);
            int lastOffset = (searchNumber+i) == nodeCount - 1 ?
                serializedTree.length : in.readInt();
            if (mbr.isIntersected(query)) {
              if (searchNumber < nonLeafNodeCount) {
                // Search child nodes
                toBeSearched.add((searchNumber + i) * degree + 1);
              } else {
                // Search all elements in this node
                toBeSearched.add(dataOffset);
                toBeSearched.add(lastOffset);
              }
            }
            dataOffset = lastOffset;
          }
        } else {
          // Search for data items (records)
          int firstOffset = searchNumber;
          int lastOffset = toBeSearched.poll();
          // Seek to firstOffset
          in.reset(); in.skip(firstOffset);
          long avail = in.available();
          // while (bytes read so far < total bytes that need to be read)
          while ((avail - in.available()) < (lastOffset - firstOffset)) {
            stockObject.readFields(in);
            LOG.info("Comparing with the leaf object: "+stockObject);
            if (stockObject.isIntersected(query)) {
              resultSize++;
              if (output != null)
                output.add(stockObject);
            }
          }
        }
      }
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return resultSize;
  }

  int knn(Point pt, int k, ResultCollector<T> results) {
    return root != null? knnMemory(pt, k, results) : knnDisk(pt, k, results);
  }

  int knnMemory(Point pt, int k, ResultCollector<T> results) {
    // TODO
    return 0;
  }

  int knnDisk(Point pt, int k, ResultCollector<T> results) {
    return 0;
  }
  
  public static void main(String[] args) throws IOException {
    long t1, t2;
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path testfile = new Path("test.rtree");
    Random random = new Random();
    // Test the size of RTree serialization
    final int degree = 11; // degree fan out
    final int record_size = 40;
    final int block_size = 32*1024*1024;
    // Total number of records
    final int record_count = getBlockCapacity(block_size, degree, record_size);
    System.out.println("Generating "+record_count+" records");
    
    Rectangle mbr = new Rectangle(0, 0, 10000, 10000);
    final Rectangle query = new Rectangle(mbr.x, mbr.y, 100, 100);
    
    RTree<TigerShape> rtree = new RTree<TigerShape>();
    TigerShape[] values = new TigerShape[record_count];
    t1 = System.currentTimeMillis();
    for (int i = 0; i < record_count; i++) {
      TigerShape s = new TigerShape();
      
      // Generate a random rectangle
      s.x = Math.abs(random.nextLong() % mbr.width) + mbr.x;
      s.y = Math.abs(random.nextLong() % mbr.height) + mbr.y;
      s.width = Math.min(Math.abs(random.nextLong() % 100) + 1,
          mbr.width + mbr.x - s.x);
      s.height = Math.min(Math.abs(random.nextLong() % 100) + 1,
          mbr.height + mbr.y - s.y);
      s.id = i;

      values[i] = s;
    }
    t2 = System.currentTimeMillis();
    System.out.println("Generated rectangles in: "+(t2-t1)+" millis");
    
    t1 = System.currentTimeMillis();
    rtree.bulkLoad(values, degree);
    t2 = System.currentTimeMillis();
    values = null;
    System.out.println("Time for bulk loading: "+(t2-t1)+" millis");

    // Write to disk
    t1 = System.currentTimeMillis();
    FSDataOutputStream dataout = fs.create(testfile, true);
    rtree.write(dataout);
    dataout.close();
    t2 = System.currentTimeMillis();
    System.out.println("Time for disk write: "+(t2-t1)+" millis");
    
    int resultCount = rtree.search(query, null);
    int height = (int)Math.ceil(Math.log(record_count)/Math.log(degree)); // Number of nodes
    long N = (long) ((Math.pow(degree, height) - 1) / (degree - 1));

    System.out.println("Selectivity: "+resultCount+" ("+((double)resultCount/record_count)+")");
    System.out.println("Expected node count: " + N + " Vs actua node count: " + rtree.getNodeCount());
    System.out.println("Expected RTree height: " + height + " Vs Actual RTree height: " + rtree.getTreeHeight());

    long expectedSize = 4 + TreeHeaderSize + record_count*record_size + N*NodeSize;
    long actualSize = new File("test.rtree").length();
    System.out.println("Expected size: "+expectedSize+" Vs Actual size: "+actualSize);
    System.out.println("Overhead than expected: "+ 
        ((actualSize-expectedSize) * 100/expectedSize) + "%");
    if (record_count > 0)
      System.out.println("Total overhead (increase in size): "+ 
          (100*actualSize/(record_size*record_count))+"%");

    // No longer need this tree. Free up memory
    values = null;
    rtree = null;

    t1 = System.currentTimeMillis();
    // Calculate total time for reading and parsing the tree
    RTree<TigerShape> rtree2 = new RTree<TigerShape>();
    rtree2.setStockObject(new TigerShape());
    
    FSDataInputStream datain = fs.open(testfile);
    rtree2.readFields(datain);
    datain.close();
    
    t2 = System.currentTimeMillis();
    System.out.println("Total time for reading and parsing the rtree from disk:"
        +(t2-t1)+" millis");

    t1 = System.currentTimeMillis();
    resultCount = rtree2.search(query, null);
    t2 = System.currentTimeMillis();
    System.out.println("Time for a small query: "+(t2-t1)+" millis");
    System.out.println("Selectivity: "+resultCount+" ("+((double)resultCount/record_count)+")");
  }
}
