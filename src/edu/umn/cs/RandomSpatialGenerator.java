package edu.umn.cs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.GridRecordWriter;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.spatial.ShapeRecordWriter;

import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.operations.Repartition;

public class RandomSpatialGenerator {
  static byte[] NEW_LINE;
  
  static {
    try {
      NEW_LINE = System.getProperty("line.separator").getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Generates a grid file in the output file system. This function uses
   * either GridRecordWriter or RTreeGridRecordWriter according to the last
   * parameter. The size of the generated file depends mainly on the number
   * of cells in the generated file and the distribution of the data. The size
   * itself doesn't make much sense because there might be some cells that are
   * not filled up with data while still having the same file size. Think of it
   * as a cabinet with some empty drawers. The cabinet size is still the same,
   * but what really makes sense here is the actual files stored in the cabinet.
   * For this, we use the total size as a hint and it means actually the
   * accumulated size of all generated shapes. It is the size of the file if
   * it were generated as a heap file. This also makes the size of the output
   * file (grid file) comparable with that of the heap file.
   * @param outFS
   * @param outFilePath
   * @param mbr
   * @param totalSize
   * @param rtree
   * @throws IOException
   */
  public static void generateGridFile(FileSystem outFS, Path outFilePath,
      Rectangle mbr, long totalSize, boolean overwrite, boolean rtree) throws IOException {
    GridInfo gridInfo = new GridInfo(mbr.x, mbr.y, mbr.width, mbr.height);
    Configuration conf = outFS.getConf();
    gridInfo.calculateCellDimensions((long)(totalSize *
        (1+conf.getFloat(Repartition.REPLICATION_OVERHEAD, 0.002f))),
        outFS.getDefaultBlockSize());
    ShapeRecordWriter recordWriter = rtree ?
        new RTreeGridRecordWriter(outFS, outFilePath, gridInfo.getAllCells(), overwrite)
        : new GridRecordWriter(outFS, outFilePath, gridInfo.getAllCells(), overwrite);

    long generatedSize = 0;
    TigerShape randomShape = new TigerShape();
    randomShape.id = Long.MAX_VALUE / 2;
    Random random = new Random();
    Text text = new Text();
    
    long t1 = System.currentTimeMillis();
    final int MaxShapeWidth = 100;
    final int MaxShapeHeight = 100;
    while (true) {
      // Generate a random rectangle
      randomShape.x = Math.abs(random.nextLong()) % (mbr.width - MaxShapeWidth) + mbr.x;
      randomShape.y = Math.abs(random.nextLong()) % (mbr.height - MaxShapeHeight) + mbr.y;
      randomShape.width = Math.abs(random.nextLong()) % MaxShapeWidth;
      randomShape.height = Math.abs(random.nextLong()) % MaxShapeHeight;
      randomShape.id++; // The ID doesn't need to be random but unique
      
      // Serialize it to text first to make it easy count its size
      text.clear();
      randomShape.toText(text);
      if (text.getLength() + NEW_LINE.length + generatedSize > totalSize)
        break;
      
      recordWriter.write((LongWritable)null, randomShape, text);
      
      generatedSize += text.getLength() + NEW_LINE.length;
    }
    long t2 = System.currentTimeMillis();
    recordWriter.close();
    long t3 = System.currentTimeMillis();
    System.out.println("Core time: "+(t2-t1)+" millis");
    System.out.println("Close time: "+(t3-t2)+" millis");
  }
  
  /**
   * Generates random rectangles and write the result to a file.
   * @param outFS - The file system that contains the output file
   * @param outputFile - The file name to write to. If either outFS or
   *   outputFile is null, data is generated to the standard output
   * @param mbr - The whole MBR to generate in
   * @param totalSize - The total size of the generated file
   * @throws IOException 
   */
  public static void generateHeapFile(FileSystem outFS, Path outputFilePath,
      Rectangle mbr, long totalSize, boolean overwrite) throws IOException {
    OutputStream out = null;
    if (outFS == null || outputFilePath == null)
      out = new BufferedOutputStream(System.out);
    else
      out = new BufferedOutputStream(outFS.create(outputFilePath, true));
    long generatedSize = 0;
    TigerShape randomShape = new TigerShape();
    randomShape.id = 0x1000000;
    Random random = new Random();
    Text text = new Text();
    
    long t1 = System.currentTimeMillis();
    while (true) {
      // Generate a random rectangle
      randomShape.x = Math.abs(random.nextLong()) % mbr.width + mbr.x;
      randomShape.y = Math.abs(random.nextLong()) % mbr.height + mbr.y;
      randomShape.width = Math.min(Math.abs(random.nextLong()) % 100,
          mbr.width + mbr.x - randomShape.x);
      randomShape.height = Math.min(Math.abs(random.nextLong()) % 100,
          mbr.height + mbr.y - randomShape.y);
      randomShape.id++; // The ID doesn't need to be random but unique
      
      // Serialize it to text first to make it easy count its size
      text.clear();
      randomShape.toText(text);
      if (text.getLength() + NEW_LINE.length + generatedSize > totalSize)
        break;
      byte[] bytes = text.getBytes();
      out.write(bytes, 0, text.getLength());
      out.write(NEW_LINE);
      generatedSize += text.getLength() + NEW_LINE.length;
    }
    long t2 = System.currentTimeMillis();
    
    // Cannot close standard output
    if (outFS != null && outputFilePath != null)
      out.close();
    else
      out.flush();
    long t3 = System.currentTimeMillis();
    System.out.println("Core time: "+(t2-t1)+" millis");
    System.out.println("Close time: "+(t3-t2)+" millis");
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(RandomSpatialGenerator.class);
    CommandLineArguments cla = new CommandLineArguments(args);
    Path outputFile = cla.getFilePath();
    FileSystem fs = outputFile != null? outputFile.getFileSystem(conf) : null;
    GridInfo grid = cla.getGridInfo();
    Rectangle mbr = cla.getRectangle();
    if (mbr == null)
      mbr = grid.getMBR();
    long totalSize = cla.getSize();
    boolean rtree = cla.isRtree();
    boolean overwrite = cla.isOverwrite();

    if (outputFile != null) {
      System.out.print("Generating a ");
      System.out.print(grid != null || rtree? "grid ": "heap ");
      System.out.println("file of size: "+totalSize);
      System.out.println("To: " + outputFile);
      System.out.println("In the range: " + mbr);
    }
    if (grid != null || rtree)
      generateGridFile(fs, outputFile, mbr, totalSize, overwrite, rtree);
    else
      generateHeapFile(fs, outputFile, mbr, totalSize, overwrite);
  }

}
