package org.apache.hadoop.spatial;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataOutputStream;
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
  
  /**
   * A node in the RTree
   * @author eldawy
   *
   */
  class Node extends Rectangle {
    // Children of each node could be either other nodes or elements
    Vector<Shape> children;
    // When we read elements from disk and we want to delay parsing values,
    // we use element data to store the raw data of all children.
    // Only when this node is queried, we parse these values and store them
    // in children list
    byte[] elementData;
    boolean leaf;
    
    private Node() {
    }
    
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

  /**Size of a non-leaf node. Four dimensions (x, y, width, height)*/
  private static final int NonLeafNodeSize = 8 * 4;

  /**Size of a leaf node. We add offset of the first element in file*/
  private static final int LeafNodeSize = NonLeafNodeSize + 4;
  
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
      static final int DIRECTION_X = 0;
      static final int DIRECTION_Y = 1;
      
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
    // Create a separate stream for node (structure) and elements (data)
    ByteArrayOutputStream nodeStream;
    FSDataOutputStream nodeOut = new FSDataOutputStream(
        nodeStream = new ByteArrayOutputStream(), null);

    ByteArrayOutputStream dataStream;
    FSDataOutputStream dataOut = new FSDataOutputStream(
        dataStream = new ByteArrayOutputStream(), null);
    
    // A list of nodes to be written do disk. Nodes are written in their
    // level-order traversal
    Queue<Node> toBeWritten = new LinkedList<Node>();
    long offsetOfFirstElement = 0;
    
    if (root != null) {
      int height = getTreeHeight();
      int degree = root.children.size();
      int nodeCount = degree == 1 && height == 1 ?
          1 : (int) ((Math.pow(degree, height) - 1) / (degree - 1));
      int leafNodeCount = (int) Math.pow(degree, height - 1);
      if (nodeCount != getNodeCount())
        throw new RuntimeException("Node count "+nodeCount+" != "+getNodeCount());
      
      // Write tree header
      nodeOut.writeInt(height);
      nodeOut.writeInt(degree);
      nodeOut.writeInt(getElementCount());
      
      offsetOfFirstElement = TreeHeaderSize +
          NonLeafNodeSize * (nodeCount - leafNodeCount)+
          LeafNodeSize * (leafNodeCount);
      
      // Start writing elements from the root
      toBeWritten.add(root);
    } else {
      int height = 0;
      offsetOfFirstElement = 4;
      nodeOut.writeInt(height); // 0 height indicates an empty tree 
    }

    while (!toBeWritten.isEmpty()) {
      // Get next element to be written
      Node n = toBeWritten.poll();
      
      if (!n.leaf) {
        // Write MBR of this node
        n.getMBR().write(nodeOut);

        // Serialize all child nodes
        for (Shape child : n.children) {
          toBeWritten.add((Node)child);
        }
      } else {
        // For leaves. Also store the offset of the first element in this
        // node
        nodeOut.writeInt((int) (offsetOfFirstElement + dataOut.getPos()));
        n.getMBR().write(nodeOut);
        
        // Then, write elements to data out
        for (Shape s : n.children) {
          s.write(dataOut);
        }
      }
    }
    if (nodeOut.getPos() != offsetOfFirstElement)
      throw new RuntimeException("Actual pos: "+nodeOut.getPos()+" != Expected post "+offsetOfFirstElement);
    long treeSize = nodeOut.getPos() + dataOut.getPos();
    nodeOut.close();
    dataOut.close();
    
    // Really write to the output stream
    out.writeLong(treeSize);
    out.write(nodeStream.toByteArray());
    out.write(dataStream.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    long treeSize = in.readLong();
    if (treeSize == 0) {
      serializedTree = null;
    } else {
      serializedTree = new byte[(int) treeSize];
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
    Rectangle[] rectangles = new Rectangle[gridInfo.getGridColumns() * gridInfo.getGridRows()];
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
    for (int col = 0; col < gridInfo.getGridColumns(); col++) {
      int xindex2 = sample.length * (col + 1) / gridInfo.getGridColumns();
      
      // Determine extents for all rectangles in this column
      long x1 = sample[xindex1].x;
      long x2 = sample[xindex2-1].x;
      
      // Sort all points in this column according to its y-coordinate
      quickSort.sort(sortableY, xindex1, xindex2);
      
      // Create rectangles in this column
      int yindex1 = xindex1;
      for (int row = 0; row < gridInfo.getGridRows(); row++) {
        int yindex2 = xindex1 + (xindex2 - xindex1) *
            (row + 1) / gridInfo.getGridRows();
        long y1 = sample[yindex1].y;
        long y2 = sample[yindex2-1].y;
        
        rectangles[iRectangle++] = new Rectangle(x1, y1, x2-x1, y2-y1);
        yindex1 = yindex2;
      }
      
      xindex1 = xindex2;
    }
    return null;
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
  
  protected int searchDisk(Rectangle query, ResultCollector<T> results) {
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
      int elementCount = in.readInt();

      Queue<Integer> toBeSearched = new LinkedList<Integer>();
      toBeSearched.add(0);

      Rectangle mbr = new Rectangle();

      while (!toBeSearched.isEmpty()) {
        int searchNumber = toBeSearched.poll();
        int mbrsToTest = searchNumber == 0 ? 1 : degree;
        if (searchNumber < nonLeafNodeCount){
          long nodeOffset = NodeNumberToOffset(searchNumber, degree,
              nonLeafNodeCount);
          in.reset(); in.skip(nodeOffset);

          // Search non-leaf (internal) nodes.
          for (int i = 0; i < mbrsToTest; i++) {
            mbr.readFields(in);
            if (mbr.isIntersected(query)) {
              toBeSearched.add((searchNumber + i) * degree + 1);
            }
          }
        } else if (searchNumber < nodeCount) {
          long nodeOffset = NodeNumberToOffset(searchNumber, degree,
              nonLeafNodeCount);
          in.reset(); in.skip(nodeOffset);
          int dataOffset = in.readInt();
          // Search nodes at leaf level. Test all siblings.
          for (int i = 0; i < mbrsToTest; i++) {
            mbr.readFields(in);
            int lastOffset = (searchNumber+i) == nodeCount - 1 ?
                serializedTree.length : in.readInt();
            if (mbr.isIntersected(query)) {
              // Search all elements in this node
              toBeSearched.add(dataOffset);
              toBeSearched.add(lastOffset);
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
            if (stockObject.isIntersected(query)) {
              resultSize++;
              if (results != null)
                results.add(stockObject);
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

  /**
   * Transforms node number (0-based) to the offset in file where this node
   * is stored.
   * @param nodeNumber - 0-based index of the node in the level-order traversal
   * @param degree - Degree of the tree. We assume all non-leaf nodes have the
   *   same degree
   * @param nonLeafNodeCount - Number of non-leaf nodes
   * @return
   */
  private static long NodeNumberToOffset(int nodeNumber, int degree,
      int nonLeafNodeCount) {
  long nodeOffset = TreeHeaderSize;
    if (nodeNumber < nonLeafNodeCount) {
      nodeOffset += NonLeafNodeSize * nodeNumber;
    } else {
      nodeOffset += nonLeafNodeCount * NonLeafNodeSize;
      nodeOffset += (nodeNumber - nonLeafNodeCount) * LeafNodeSize;
    }
    return nodeOffset;
  }

  int knn(Point pt, int k, ResultCollector<T> results) {
    return root != null? knnMemory(pt, k, results) : knnDisk(pt, k, results);
  }

  int knnMemory(Point pt, int k, ResultCollector<T> results) {
    // TODO louai
    return 0;
  }

  int knnDisk(Point pt, int k, ResultCollector<T> results) {
    return 0;
  }
  
  public static void main(String[] args) throws IOException {
    long t1, t2;
    Random random = new Random();
    // Test the size of RTree serialization
    final int R = 1000000; // Total number of records
    final int d = 5; // degree fan out
    final int recordSize = 42;
    final int nodeSize = 42;
    Rectangle mbr = new Rectangle(0, 0, 10000, 10000);
    final Rectangle query = new Rectangle(mbr.x, mbr.y, 10000, 10000);
    
    RTree<TigerShape> rtree = new RTree<TigerShape>();
    TigerShape[] values = new TigerShape[R];
    
    t1 = System.currentTimeMillis();
    for (int i = 0; i < R; i++) {
      Rectangle r = new Rectangle();
      TigerShape s = new TigerShape(r, i);
      
      // Generate a random rectangle
      r.x = Math.abs(random.nextLong() % mbr.width) + mbr.x;
      r.y = Math.abs(random.nextLong() % mbr.height) + mbr.y;
      r.width = Math.min(Math.abs(random.nextLong() % 100) + 1,
          mbr.width + mbr.x - r.x);
      r.height = Math.min(Math.abs(random.nextLong() % 100) + 1,
          mbr.height + mbr.y - r.y);

      values[i] = s;
    }
    t2 = System.currentTimeMillis();
    System.out.println("Generated rectangles in: "+(t2-t1)+" millis");
    
    t1 = System.currentTimeMillis();
    rtree.bulkLoad(values, d);
    t2 = System.currentTimeMillis();
    System.out.println("Time for bulk loading: "+(t2-t1)+" millis");

    // Write to disk
    t1 = System.currentTimeMillis();
    DataOutputStream dataout = new FSDataOutputStream(new BufferedOutputStream(new FileOutputStream("test.rtree")), null);
    rtree.write(dataout);
    dataout.close();
    t2 = System.currentTimeMillis();
    System.out.println("Time for disk write: "+(t2-t1)+" millis");
    
    int resultCount = rtree.search(query, null);
    int height = (int)Math.ceil(Math.log(R)/Math.log(d)); // Number of nodes
    long N = (long) ((Math.pow(d, height) - 1) / (d - 1));

    System.out.println("Selectivity: "+resultCount+" ("+((double)resultCount/R)+")");
    System.out.println("Expected node count: " + N + " Vs actua node count: " + rtree.getNodeCount());
    System.out.println("Expected RTree height: " + height + " Vs Actual RTree height: " + rtree.getTreeHeight());

    long expectedSize = TreeHeaderSize + R*recordSize + N*LeafNodeSize;
    long actualSize = new File("test.rtree").length();
    System.out.println("Expected size: "+expectedSize+" Vs Actual size: "+actualSize);
    System.out.println("Overhead than expected: "+ 
        ((actualSize-expectedSize) * 100/expectedSize) + "%");
    if (R > 0)
      System.out.println("Total overhead (increase in size): "+ 
          (100*actualSize/(recordSize*R))+"%");

    t1 = System.currentTimeMillis();
    // Calculate total time for reading and parsing the tree
    RTree<TigerShape> rtree2 = new RTree<TigerShape>();
    rtree2.setStockObject(new TigerShape(new Rectangle(), 1));
    
    DataInputStream datain = new DataInputStream(new BufferedInputStream(new FileInputStream("test.rtree")));
    rtree2.readFields(datain);
    datain.close();
    
    t2 = System.currentTimeMillis();
    System.out.println("Total time for reading and parsing the rtree from disk:"
        +(t2-t1)+" millis");

    t1 = System.currentTimeMillis();
    resultCount = rtree2.search(query, null);
    t2 = System.currentTimeMillis();
    System.out.println("Time for a small query: "+(t2-t1)+" millis");
    System.out.println("Selectivity: "+resultCount+" ("+((double)resultCount/R)+")");
    
  }
}
