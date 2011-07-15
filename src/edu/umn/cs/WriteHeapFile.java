package edu.umn.cs;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatial.TigerShape;

public class WriteHeapFile {

	/**An output stream for each grid cell*/
  /**Configuration used to communicate with Hadoop/HDFS*/
  private static Configuration conf;
  private static FileSystem fileSystem;

  public static void writeFile(String inputFilename, Path outputPath,
      Class<Shape> shapeClass) throws IOException,
      InstantiationException, IllegalAccessException {
    // Get HDFS instance with default configuration
    conf = new Configuration();
    fileSystem = FileSystem.get(conf);

    OutputStream os = fileSystem.create(outputPath, true);
    
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
      line.clear();
      shape.toText(line);
      os.write(line.getBytes(), 0, line.getLength());
      os.write('\n');
    }
    
    System.out.println("Total lines: "+totalLines);
    
    // Close input file
    reader.close();

    // Close output file
    os.close();
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
	  String inputFilename = args[0];
		Path outputPath = new Path(args[1]);

		String shapeClassName = Shape.class.getName();
		String shapeName = args.length > 2 ? args[2] : "Rectangle";
		shapeClassName = shapeClassName.replace("Shape", shapeName);
		
		writeFile(inputFilename, outputPath, (Class<Shape>) Class.forName(shapeClassName));
	}
}
