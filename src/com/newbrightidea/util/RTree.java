package com.newbrightidea.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * Implementation of an arbitrary-dimension RTree. Based on R-Trees: A Dynamic
 * Index Structure for Spatial Searching (Antonn Guttmann, 1984)
 * 
 * This class is not thread-safe.
 * 
 * Copyright 2010 Russ Weeks rweeks@newbrightidea.com Licensed under the GNU
 * LGPL License details here: http://www.gnu.org/licenses/lgpl-3.0.txt
 * 
 * @param <T>
 *          the type of entry to store in this RTree.
 */
public class RTree<T> implements Writable
{

  private int maxEntries;
  private int minEntries;
  private int numDims;

  private Node root;

  private volatile int size;

  /**
   * Creates a new RTree.
   * 
   * @param maxEntries
   *          maximum number of entries per node
   * @param minEntries
   *          minimum number of entries per node (except for the root node)
   * @param numDims
   *          the number of dimensions of the RTree.
   */
  public RTree(int maxEntries, int minEntries, int numDims)
  {
    assert (minEntries <= (maxEntries / 2));
    this.numDims = numDims;
    this.maxEntries = maxEntries;
    this.minEntries = minEntries;
    root = buildRoot(true);
  }

  private Node buildRoot(boolean asLeaf)
  {
    long[] initCoords = new long[numDims];
    long[] initDimensions = new long[numDims];
    for (int i = 0; i < this.numDims; i++)
    {
      initCoords[i] = (long) Math.sqrt(Long.MAX_VALUE);
      initDimensions[i] = -2 * (long) Math.sqrt(Long.MAX_VALUE);
    }
    return new Node(initCoords, initDimensions, asLeaf);
  }

  /**
   * Builds a new RTree using default parameters: maximum 50 entries per node
   * minimum 2 entries per node 2 dimensions
   */
  public RTree()
  {
    this(50, 2, 2);
  }

  /**
   * @return the maximum number of entries per node
   */
  public int getMaxEntries()
  {
    return maxEntries;
  }

  /**
   * @return the minimum number of entries per node for all nodes except the
   *         root.
   */
  public int getMinEntries()
  {
    return minEntries;
  }

  /**
   * @return the number of dimensions of the tree
   */
  public int getNumDims()
  {
    return numDims;
  }

  /**
   * Searches the RTree for objects overlapping with the given rectangle.
   * 
   * @param coords
   *          the corner of the rectangle that is the lower bound of every
   *          dimension (eg. the top-left corner)
   * @param dimensions
   *          the dimensions of the rectangle.
   * @return a list of objects whose rectangles overlap with the given
   *         rectangle.
   */
  public List<T> search(long[] coords, long[] dimensions)
  {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    LinkedList<T> results = new LinkedList<T>();
    search(coords, dimensions, root, results);
    return results;
  }

  private void search(long[] coords, long[] dimensions, Node n,
      LinkedList<T> results)
  {
    if (n.leaf)
    {
      for (Node e : n.children)
      {
        if (isOverlap(coords, dimensions, e.coords, e.dimensions))
        {
          results.add(((Entry) e).entry);
        }
      }
    }
    else
    {
      for (Node c : n.children)
      {
        if (isOverlap(coords, dimensions, c.coords, c.dimensions))
        {
          search(coords, dimensions, c, results);
        }
      }
    }
  }

  /**
   * Deletes the entry associated with the given rectangle from the RTree
   * 
   * @param coords
   *          the corner of the rectangle that is the lower bound in every
   *          dimension
   * @param dimensions
   *          the dimensions of the rectangle
   * @param entry
   *          the entry to delete
   * @return true iff the entry was deleted from the RTree.
   */
  public boolean delete(long[] coords, long[] dimensions, T entry)
  {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    Node l = findLeaf(root, coords, dimensions, entry);
    assert (l.leaf);
    ListIterator<Node> li = l.children.listIterator();
    T removed = null;
    while (li.hasNext())
    {
      @SuppressWarnings("unchecked")
      Entry e = (Entry) li.next();
      if (e.entry.equals(entry))
      {
        removed = e.entry;
        li.remove();
        break;
      }
    }
    if (removed != null)
    {
      condenseTree(l);
      size--;
    }
    return (removed != null);
  }

  private Node findLeaf(Node n, long[] coords, long[] dimensions, T entry)
  {
    if (n.leaf)
    {
      for (Node c : n.children)
      {
        if (((Entry) c).entry.equals(entry))
        {
          return n;
        }
      }
      return null;
    }
    else
    {
      for (Node c : n.children)
      {
        if (isOverlap(c.coords, c.dimensions, coords, dimensions))
        {
          Node result = findLeaf(c, coords, dimensions, entry);
          if (result != null)
          {
            return result;
          }
        }
      }
      return null;
    }
  }

  private void condenseTree(Node n)
  {
    Set<Node> q = new HashSet<Node>();
    while (n != root)
    {
      if (n.leaf && (n.children.size() < minEntries))
      {
        q.addAll(n.children);
        n.parent.children.remove(n);
      }
      else if (!n.leaf && (n.children.size() < minEntries))
      {
        // probably a more efficient way to do this...
        LinkedList<Node> toVisit = new LinkedList<Node>(n.children);
        while (!toVisit.isEmpty())
        {
          Node c = toVisit.pop();
          if (c.leaf)
          {
            q.addAll(c.children);
          }
          else
          {
            toVisit.addAll(c.children);
          }
        }
        n.parent.children.remove(n);
      }
      else
      {
        tighten(n);
      }
      n = n.parent;
    }
    for (Node ne : q)
    {
      @SuppressWarnings("unchecked")
      Entry e = (Entry) ne;
      insert(e.coords, e.dimensions, e.entry);
    }
  }

  /**
   * Inserts the given entry into the RTree, associated with the given
   * rectangle.
   * 
   * @param coords
   *          the corner of the rectangle that is the lower bound in every
   *          dimension
   * @param dimensions
   *          the dimensions of the rectangle
   * @param entry
   *          the entry to insert
   */
  public void insert(long[] coords, long[] dimensions, T entry)
  {
    assert (coords.length == numDims);
    assert (dimensions.length == numDims);
    Entry e = new Entry(coords, dimensions, entry);
    Node l = chooseLeaf(root, e);
    l.children.add(e);
    size++;
    e.parent = l;
    if (l.children.size() > maxEntries)
    {
      Node[] splits = splitNode(l);
      adjustTree(splits[0], splits[1]);
    }
    else
    {
      adjustTree(l, null);
    }
  }

  /**
   * Empties the RTree
   */
  public void clear()
  {
    root = buildRoot(true);
    // let the GC take care of the rest.
  }
  
  private void adjustTree(Node n, Node nn)
  {
    if (n == root)
    {
      if (nn != null)
      {
        // build new root and add children.
        root = buildRoot(false);
        root.children.add(n);
        n.parent = root;
        root.children.add(nn);
        nn.parent = root;
      }
      tighten(root);
      return;
    }
    tighten(n);
    if (nn != null)
    {
      tighten(nn);
      if (n.parent.children.size() > maxEntries)
      {
        Node[] splits = splitNode(n.parent);
        adjustTree(splits[0], splits[1]);
      }
    }
    else if (n.parent != null)
    {
      adjustTree(n.parent, null);
    }
  }

  private Node[] splitNode(Node n)
  {
    @SuppressWarnings("unchecked")
    Node[] nn = new RTree.Node[]
    { n, new Node(n.coords, n.dimensions, n.leaf) };
    nn[1].parent = n.parent;
    if (nn[1].parent != null)
    {
      nn[1].parent.children.add(nn[1]);
    }
    LinkedList<Node> cc = new LinkedList<Node>(n.children);
    n.children.clear();
    Node[] ss = pickSeeds(cc);
    nn[0].children.add(ss[0]);
    nn[1].children.add(ss[1]);
    while (!cc.isEmpty())
    {
      if ((nn[0].children.size() >= minEntries)
          && (nn[1].children.size() + cc.size() == minEntries))
      {
        nn[1].children.addAll(cc);
        cc.clear();
        return nn;
      }
      else if ((nn[1].children.size() >= minEntries)
          && (nn[0].children.size() + cc.size() == minEntries))
      {
        nn[0].children.addAll(cc);
        cc.clear();
        return nn;
      }
      Node c = cc.pop();
      Node preferred;
      // Implementation of linear PickNext
      long e0 = getRequiredExpansion(nn[0].coords, nn[0].dimensions, c);
      long e1 = getRequiredExpansion(nn[1].coords, nn[1].dimensions, c);
      if (e0 < e1)
      {
        preferred = nn[0];
      }
      else if (e0 > e1)
      {
        preferred = nn[1];
      }
      else
      {
        long a0 = getArea(nn[0].dimensions);
        long a1 = getArea(nn[1].dimensions);
        if (a0 < a1)
        {
          preferred = nn[0];
        }
        else if (e0 > a1)
        {
          preferred = nn[1];
        }
        else
        {
          if (nn[0].children.size() < nn[1].children.size())
          {
            preferred = nn[0];
          }
          else if (nn[0].children.size() > nn[1].children.size())
          {
            preferred = nn[1];
          }
          else
          {
            preferred = nn[(int) Math.round(Math.random())];
          }
        }
      }
      preferred.children.add(c);
    }
    tighten(nn[0]);
    tighten(nn[1]);
    return nn;
  }

  // Implementation of LinearPickSeeds
  private RTree<T>.Node[] pickSeeds(LinkedList<Node> nn)
  {
    RTree<T>.Node[] bestPair = null;
    long bestSep = 0;
    for (int i = 0; i < numDims; i++)
    {
      float dimLb = Float.MAX_VALUE, dimMinUb = Float.MAX_VALUE;
      float dimUb = -1.0f * Float.MAX_VALUE, dimMaxLb = -1.0f * Float.MAX_VALUE;
      Node nMaxLb = null, nMinUb = null;
      for (Node n : nn)
      {
        if (n.coords[i] < dimLb)
        {
          dimLb = n.coords[i];
        }
        if (n.dimensions[i] + n.coords[i] > dimUb)
        {
          dimUb = n.dimensions[i] + n.coords[i];
        }
        if (n.coords[i] > dimMaxLb)
        {
          dimMaxLb = n.coords[i];
          nMaxLb = n;
        }
        if (n.dimensions[i] + n.coords[i] < dimMinUb)
        {
          dimMinUb = n.dimensions[i] + n.coords[i];
          nMinUb = n;
        }
      }
      long sep = (nMaxLb == nMinUb) ? 0 :
                  (long)Math.abs((dimMinUb - dimMaxLb) / (dimUb - dimLb));
      if (sep >= bestSep)
      {
        bestPair = new RTree.Node[]
        { nMaxLb, nMinUb };
        bestSep = sep;
      }
    }
    // In the degenerate case where all points are the same, the above
    // algorithm does not find a best pair.  Just pick the first 2
    // children.
    if ( bestPair == null )
    {
      bestPair = new RTree.Node[] { nn.get(0), nn.get(1) };
    }
    nn.remove(bestPair[0]);
    nn.remove(bestPair[1]);
    return bestPair;
  }

  private void tighten(Node n)
  {
    long[] minCoords = new long[n.coords.length];
    long[] maxDimensions = new long[n.dimensions.length];
    for (int i = 0; i < minCoords.length; i++)
    {
      minCoords[i] = Long.MAX_VALUE;
      maxDimensions[i] = 0;

      for (Node c : n.children)
      {
        // we may have bulk-added a bunch of children to a node (eg. in
        // splitNode)
        // so here we just enforce the child->parent relationship.
        c.parent = n;
        if (c.coords[i] < minCoords[i])
        {
          minCoords[i] = c.coords[i];
        }
        if ((c.coords[i] + c.dimensions[i]) > maxDimensions[i])
        {
          maxDimensions[i] = (c.coords[i] + c.dimensions[i]);
        }
      }
    }
    System.arraycopy(minCoords, 0, n.coords, 0, minCoords.length);
    System.arraycopy(maxDimensions, 0, n.dimensions, 0, maxDimensions.length);
  }

  private RTree<T>.Node chooseLeaf(RTree<T>.Node n, RTree<T>.Entry e)
  {
    if (n.leaf)
    {
      return n;
    }
    long minInc = Long.MAX_VALUE;
    Node next = null;
    for (RTree<T>.Node c : n.children)
    {
      long inc = getRequiredExpansion(c.coords, c.dimensions, e);
      if (inc < minInc)
      {
        minInc = inc;
        next = c;
      }
      else if (inc == minInc)
      {
        long curArea = 1;
        long thisArea = 1;
        for (int i = 0; i < c.dimensions.length; i++)
        {
          curArea *= next.dimensions[i];
          thisArea *= c.dimensions[i];
        }
        if (thisArea < curArea)
        {
          next = c;
        }
      }
    }
    return chooseLeaf(next, e);
  }

  /**
   * Returns the increase in area necessary for the given rectangle to cover the
   * given entry.
   */
  private long getRequiredExpansion(long[] coords, long[] dimensions, Node e)
  {
    long area = getArea(dimensions);
    long[] deltas = new long[dimensions.length];
    for (int i = 0; i < deltas.length; i++)
    {
      if (coords[i] + dimensions[i] < e.coords[i] + e.dimensions[i])
      {
        deltas[i] = e.coords[i] + e.dimensions[i] - coords[i] - dimensions[i];
      }
      else if (coords[i] + dimensions[i] > e.coords[i] + e.dimensions[i])
      {
        deltas[i] = coords[i] - e.coords[i];
      }
    }
    long expanded = 1;
    for (int i = 0; i < dimensions.length; i++)
    {
      area *= dimensions[i] + deltas[i];
    }
    return (expanded - area);
  }

  private long getArea(long[] dimensions)
  {
    long area = 1;
    for (int i = 0; i < dimensions.length; i++)
    {
      area *= dimensions[i];
    }
    return area;
  }

  private boolean isOverlap(long[] scoords, long[] sdimensions,
      long[] coords, long[] dimensions)
  {
    for (int i = 0; i < scoords.length; i++)
    {
      boolean overlapInThisDimension = false;
      if (scoords[i] == coords[i])
      {
        overlapInThisDimension = true;
      }
      else if (scoords[i] < coords[i])
      {
        if (scoords[i] + sdimensions[i] >= coords[i])
        {
          overlapInThisDimension = true;
        }
      }
      else if (scoords[i] > coords[i])
      {
        if (coords[i] + dimensions[i] >= scoords[i])
        {
          overlapInThisDimension = true;
        }
      }
      if (!overlapInThisDimension)
      {
        return false;
      }
    }
    return true;
  }
 
  private class Node implements Writable
  {
    long[] coords;
    long[] dimensions;
    LinkedList<Node> children;
    boolean leaf;

    Node parent;

    private Node(long[] coords, long[] dimensions, boolean leaf)
    {
      this.coords = new long[coords.length];
      this.dimensions = new long[dimensions.length];
      System.arraycopy(coords, 0, this.coords, 0, coords.length);
      System.arraycopy(dimensions, 0, this.dimensions, 0, dimensions.length);
      this.leaf = leaf;
      children = new LinkedList<Node>();
    }
    
    public Node() {
      this.coords = new long[RTree.this.numDims];
      this.dimensions = new long[RTree.this.numDims];
      this.leaf = false;
      children = new LinkedList<Node>();
    }

    @Override
    public void readFields(DataInput datain) throws IOException {
      if (coords == null)
        coords = new long[RTree.this.numDims];
      if (dimensions == null)
        dimensions = new long[RTree.this.numDims];
      for (int i = 0; i < coords.length; i++) {
        coords[i] = datain.readLong();
        dimensions[i] = datain.readLong();
      }
      int numChildren = datain.readInt();
      children.clear();
      while (numChildren-- > 0) {
        Entry entry = new Entry();
        entry.readFields(datain);
        children.add(entry);
      }
      this.leaf = children.isEmpty();
    }

    @Override
    public void write(DataOutput dataout) throws IOException {
      for (int i = 0; i < coords.length; i++) {
        dataout.writeLong(coords[i]);
        dataout.writeLong(dimensions[i]);
      }
      dataout.writeInt(children.size());
      for (Node child : children) {
        child.write(dataout);
      }
    }
  }

  private class Entry extends Node
  {
    T entry;

    public Entry(long[] coords, long[] dimensions, T entry)
    {
      // an entry isn't actually a leaf (its parent is a leaf)
      // but all the algorithms should stop at the first leaf they encounter,
      // so this little hack shouldn't be a problem.
      super(coords, dimensions, true);
      this.entry = entry;
    }

    public Entry() {
    }

    public String toString()
    {
      return "Entry: " + entry;
    }
  } 

  // The methods below this point can be used to create an HTML rendering
  // of the RTree.  Maybe useful for debugging?
  
  private static final int elemWidth = 150;
  private static final int elemHeight = 120;
  
  String visualize()
  {
    int ubDepth = (int)Math.ceil(Math.log(size)/Math.log(minEntries)) * elemHeight;
    int ubWidth = size * elemWidth;
    java.io.StringWriter sw = new java.io.StringWriter();
    java.io.PrintWriter pw = new java.io.PrintWriter(sw);
    pw.println( "<html><head></head><body>");
    visualize(root, pw, 0, 0, ubWidth, ubDepth);
    pw.println( "</body>");
    pw.flush();
    return sw.toString();
  }
  
  private void visualize(Node n, java.io.PrintWriter pw, int x0, int y0, int w, int h)
  {
    pw.printf( "<div style=\"position:absolute; left: %d; top: %d; width: %d; height: %d; border: 1px dashed\">\n",
               x0, y0, w, h);
    pw.println( "<pre>");
    pw.println( "Node: " + n.toString() + " (root==" + (n == root) + ") \n" );
    pw.println( "Coords: " + Arrays.toString(n.coords) + "\n");
    pw.println( "Dimensions: " + Arrays.toString(n.dimensions) + "\n");
    pw.println( "# Children: " + ((n.children == null) ? 0 : n.children.size()) + "\n" );
    pw.println( "isLeaf: " + n.leaf + "\n");
    pw.println( "</pre>");
    int numChildren = (n.children == null) ? 0 : n.children.size();
    for ( int i = 0; i < numChildren; i++ )
    {
      visualize(n.children.get(i), pw, (int)(x0 + (i * w/(float)numChildren)),
          y0 + elemHeight, (int)(w/(float)numChildren), h - elemHeight);
    }
    pw.println( "</div>" );
  }

  @Override
  public void readFields(DataInput datain) throws IOException {
    maxEntries = datain.readInt();
    minEntries = datain.readInt();
    numDims = datain.readInt();
    if (datain.readBoolean()) {
      root = new Entry();
      root.readFields(datain);
    }
  }

  @Override
  public void write(DataOutput dataout) throws IOException {
    dataout.writeInt(maxEntries);
    dataout.writeInt(minEntries);
    dataout.writeInt(numDims);
    dataout.writeBoolean(root != null);
    if (root != null)
      root.write(dataout);
  }
}
