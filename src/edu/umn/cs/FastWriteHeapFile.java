package edu.umn.cs;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.spatial.GridInfo;

public class FastWriteHeapFile {

	/**An output stream for each grid cell*/
  private static Path outputFilename;
  private static String inputFilename;
  /**Configuration used to communicate with Hadoop/HDFS*/
  private static Configuration conf;
  private static FileSystem fs;


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
		inputFilename = args[0];
		outputFilename = new Path(args[1]);

		// Initialize histogram
		// int [][]histogram = new int [GridColumns][GridRows];

		conf = new Configuration();
		
		fs = FileSystem.get(conf);

    // Open input file
    DataInputStream reader = new DataInputStream(new FileInputStream(inputFilename));
    FSDataOutputStream os = fs.create(outputFilename, true);

    while (reader.available() > 0) {
      long id = reader.readLong();
      int rx1 = reader.readInt();
      int ry1 = reader.readInt();
      int rx2 = reader.readInt();
      int ry2 = reader.readInt();
      int type = reader.readInt();

      // Write to HDFS
      os.writeLong(id);
      os.writeInt(rx1);
      os.writeInt(ry1);
      os.writeInt(rx2);
      os.writeInt(ry2);
      os.writeInt(type);
    }
    
    // Close input file
    reader.close();
    os.close();
    
	}
}