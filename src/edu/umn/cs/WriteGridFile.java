package edu.umn.cs;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatial.mapReduce.GridRecordWriter;

public class WriteGridFile {

	/**An output stream for each grid cell*/
  /**Configuration used to communicate with Hadoop/HDFS*/
  private static Configuration conf;
  private static FileSystem fileSystem;
  private static GridInfo gridInfo;

  public static void writeGridFile(String inputFilename, Path outputPath,
      GridInfo gridInfo, Class<Shape> shapeClass) throws IOException,
      InstantiationException, IllegalAccessException {
    // Get HDFS instance with default configuration
    conf = new Configuration();
    fileSystem = FileSystem.get(conf);

    // Automatically calculate recommended cell dimensions if not set
    // Calculate appropriate values for cellWidth, cellHeight based on file size
    // only if they're missing
    if (gridInfo.cellWidth == 0)
      gridInfo.calculateCellDimensions(new File(inputFilename).length(), fileSystem.getDefaultBlockSize());

    // Prepare grid file writer
    GridRecordWriter rrw = new GridRecordWriter(fileSystem, outputPath, gridInfo);

    // Open input file
    LineReader reader = new LineReader(new FileInputStream(inputFilename));

    TigerShape shape = new TigerShape(shapeClass.newInstance(), 0);
    LongWritable dummyId = new LongWritable();
    Text line = new Text();
    int totalLines = 0;
    while (reader.readLine(line) > 0) {
      totalLines++;
      // Parse shape dimensions
      shape.fromText(line);

      // Write to output file
      rrw.write(dummyId, shape);
    }
    
    // Close input file
    reader.close();

    // Close output file
    rrw.close(null);
  }
  
	/**
	 * Write a rectangles file to HDFS.
	 * Usage: <grid info> <source file> <destination file>
	 * grid info: xOrigin,yOrigin,gridWidth,gridHeight[,cellWidth,cellHeight]
	 * source file: Local file
	 * destination file: HDFS file
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main (String [] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
	  // Retrieve query rectangle and store it to an HDFS file
	  gridInfo = new GridInfo();
	  gridInfo.readFromString(args[0]);

	  String inputFilename = args[1];
		Path outputPath = new Path(args[2]);

		String shapeClassName = Shape.class.getName();
		String shapeName = args.length > 3 ? args[3] : "Rectangle";
		shapeClassName = shapeClassName.replace("Shape", shapeName);
		
		writeGridFile(inputFilename, outputPath, gridInfo, (Class<Shape>) Class.forName(shapeClassName));
	}
}
