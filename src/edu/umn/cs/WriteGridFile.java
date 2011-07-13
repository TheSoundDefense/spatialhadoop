package edu.umn.cs;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;

import edu.umn.cs.spatial.TigerShape;
import edu.umn.cs.spatial.mapReduce.TigerShapeRecordWriter;

public class WriteGridFile {

	/**An output stream for each grid cell*/
  /**Configuration used to communicate with Hadoop/HDFS*/
  private static Configuration conf;
  private static FileSystem fileSystem;
  private static GridInfo gridInfo;

	/**
	 * Write a rectangles file to HDFS.
	 * Usage: <grid info> <source file> <destination file>
	 * grid info: xOrigin,yOrigin,gridWidth,gridHeight,cellWidth,cellHeight
	 * source file: Local file
	 * destination file: HDFS file
	 * @param args
	 * @throws IOException
	 */
	public static void main (String [] args) throws IOException {
		String inputFilename = args[1];
		Path outputPath = new Path(args[2]);

		conf = new Configuration();
		
		fileSystem = FileSystem.get(conf);

    // Retrieve query rectangle and store it to an HDFS file
    gridInfo = new GridInfo();
    gridInfo.readFromString(args[0]);
    
    // Calculate appropriate values for cellWidth, cellHeight based on file size
    // only if they're missing
    if (gridInfo.cellWidth == 0)
      gridInfo.calculateCellDimensions(new File(inputFilename).length(), fileSystem.getDefaultBlockSize());

    // Prepare grid file writer
    TigerShapeRecordWriter rrw = new TigerShapeRecordWriter(fileSystem, outputPath, gridInfo);

    // Open input file
    LineNumberReader reader = new LineNumberReader(new FileReader(inputFilename));

    TigerShape shape = new TigerShape(new Rectangle(), 0);
    LongWritable dummyId = new LongWritable();
    while (reader.ready()) {
      // TODO we can make it faster by not doing a trim or split to avoid creating unnecessary objects
      String line = reader.readLine().trim();
      // Parse rectangle dimensions
      shape.readFromString(line);
      
      rrw.write(dummyId, shape);
    }
    
    // Close input file
    reader.close();
    
    // Close output file
    rrw.close(null);
	}
}
