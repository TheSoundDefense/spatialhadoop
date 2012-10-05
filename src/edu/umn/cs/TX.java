package edu.umn.cs;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatialHadoop.mapReduce.TigerShapeRecordReader;

public class TX {

  public static void main(String[] args) throws IOException {
    // Read all shapes in file
    Configuration conf = new Configuration();
    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
    Path inputFile = new Path("/media/scratch/edges_merge.rects");
    Path outputFile = new Path("/home/eldawy/Desktop/tx.points");
    FileSystem fs = inputFile.getFileSystem(conf);
    long length = fs.getFileStatus(inputFile).getLen();
    TigerShapeRecordReader reader = new TigerShapeRecordReader(conf,
        fs.open(inputFile), 0, length);
    LongWritable shapeId = reader.createKey();
    TigerShape shape = reader.createValue();
    Vector<Point> shapes = new Vector<Point>();
    long x1 = Long.MAX_VALUE;
    long y1 = Long.MAX_VALUE;
    long x2 = Long.MIN_VALUE;
    long y2 = Long.MIN_VALUE;
    PrintStream writer = new PrintStream(fs.create(outputFile, true));
    while (reader.next(shapeId, shape)) {
      Rectangle mbr = shape.getMBR();
      Point pt = mbr.getCenterPoint();
      shapes.add(pt);
      if (Math.random() < (1.0 / 1000.0)) {
        writer.printf("%d,%d", pt.x, pt.y);
        writer.println();
      }
      if (mbr.getX1() < x1)
        x1 = mbr.getX1();
      if (mbr.getY1() < y1)
        y1 = mbr.getY1();
      if (mbr.getX2() > x2)
        x2 = mbr.getX2();
      if (mbr.getY2() > y2)
        y2 = mbr.getY2();
      
    }
    writer.close();
    reader.close();
    
    // Partition them into a 4x4 grid
    GridInfo gridInfo = new GridInfo(x1, y1, x2-x1, y2-y1, (x2-x1)/4+1, (y2-y1)/4+1);
    Rectangle[] cells = 
        RTree.packInRectangles(gridInfo, shapes.toArray(new Point[shapes.size()]));
    
    System.out.println("@str_cells = [");
    for (Rectangle cell : cells) {
      System.out.printf("  [%d, %d, %d, %d],", cell.x, cell.y, cell.width, cell.height);
      System.out.println();
    }
    System.out.println("]");
    System.out.println();
    
    System.out.println("@grid_cells = [");
    for (Rectangle cell : gridInfo.getAllCells()) {
      System.out.printf("  [%d, %d, %d, %d],", cell.x, cell.y, cell.width, cell.height);
      System.out.println();
    }
    System.out.println("]");
    System.out.println();
    
    System.out.printf("@mbr = [%d, %d, %d, %d]", x1, y1, x2-x1, y2-y1);
    System.out.println();
  }
}
