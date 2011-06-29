package edu.umn.cs;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.spatial.GridInfo;

public class WritePointFile {
  public static final Log LOG = LogFactory.getLog(WritePointFile.class);

  private static final int BufferLength = 1024 * 1024;
  private static long BlockSize = 64 * 1024 * 1024;
	/**An output stream for each grid cell*/
  private static PrintStream[][] cellStreams;
  private static long[][] cellSizes;
  private static String outputFilename;
  private static String inputFilename;
  /**Configuration used to communicate with Hadoop/HDFS*/
  private static Configuration conf;
  private static FileSystem fs;
  private static GridInfo gridInfo;

  /**
   * Return path to a file that is used to write one grid cell
   * @param column
   * @param row
   * @return
   */
	public static Path getCellFilePath(int column, int row) {
	  return new Path(outputFilename + '_' + column + '_' + row);
	}
	
	/**
	 * Find or create a print stream used to write the point given by x, y.
	 * x, y can be any an arbitrary point in the space.
	 * The returned print stream is associated with the grid cell that contains
	 * the given point.
	 * @param x
	 * @param y
	 * @return
	 * @throws IOException
	 */
	public static PrintStream getOrCreateCellFile(double x, double y) throws IOException {
    int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
    int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
	  PrintStream ps = cellStreams[column][row];
	  if (ps == null) {
	    
	    FSDataOutputStream os = fs.create(getCellFilePath(column, row), gridInfo);
	    cellStreams[column][row] = ps = new PrintStream(os);
      double xCell = column * gridInfo.cellWidth + gridInfo.xOrigin;
      double yCell = row * gridInfo.cellHeight + gridInfo.yOrigin;
	    ((DFSOutputStream)os.getWrappedStream()).setNextBlockCell(xCell, yCell);
	  }
	  return ps;
	}

	public static void main (String [] args) throws IOException {
		inputFilename = args[1];
		outputFilename = args[2];

		// Initialize histogram
		// int [][]histogram = new int [GridColumns][GridRows];

    conf = new Configuration();
    
    fs = FileSystem.get(conf);
    BlockSize = fs.getDefaultBlockSize();

    // Retrieve query rectangle and store it to an HDFS file
    gridInfo = new GridInfo();
    String[] parts = args[0].split(",");

    gridInfo.xOrigin = Double.parseDouble(parts[0]);
    gridInfo.yOrigin = Double.parseDouble(parts[1]);
    gridInfo.gridWidth = Double.parseDouble(parts[2]);
    gridInfo.gridHeight = Double.parseDouble(parts[3]);
    gridInfo.cellWidth = Double.parseDouble(parts[4]);
    gridInfo.cellHeight = Double.parseDouble(parts[5]);
    int gridColumns = (int) Math.ceil(gridInfo.gridWidth / gridInfo.cellWidth); 
    int gridRows = (int) Math.ceil(gridInfo.gridHeight / gridInfo.cellHeight);
    
    // Prepare an array to hold all output streams
    cellStreams = new PrintStream[gridColumns][gridRows];
    cellSizes = new long[gridColumns][gridRows];

    // Open input file
    LineNumberReader reader = new LineNumberReader(new FileReader(inputFilename));

    while (reader.ready()) {
      String line = reader.readLine().trim();
      // Parse point information
      parts = line.split(",");
      //int id = Integer.parseInt(parts[0]);
      float x = Float.parseFloat(parts[1]);
      float y = Float.parseFloat(parts[2]);
      //int type = Integer.parseInt(parts[3]);

      // Write to the correct file
      PrintStream ps = getOrCreateCellFile(x, y);
      ps.println(line);
      // increase number of bytes written to this print stream
      int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
      int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
      cellSizes[column][row] += line.length() + 1;
    }
    
    // Close input file
    reader.close();
    LOG.info("All input file parsed");
    
    // Complete this block with empty lines
    // We use % because we might have written multiple blocks
    // for this grid cell
    //
    //while (remainingBytes-- > 0) {
    //  ps.println();
    //}
    
    // Create a buffer filled with new lines
    byte[] buffer = new byte[BufferLength];
    for (int i = 0; i < buffer.length; i++)
      buffer[i] = '\n';
    
    Vector<Path> pathsToConcat = new Vector<Path>();
    // Close all output files
    for (int i = 0; i < cellStreams.length; i++) {
      for (int j = 0; j < cellStreams[i].length; j++) {
        if (cellStreams[i][j] != null) {
          // Stuff all open streams with empty lines until each one is 64 MB
          long remainingBytes = BlockSize - cellSizes[i][j] % BlockSize;
          // Write some bytes so that remainingBytes is multiple of buffer.length
          cellStreams[i][j].write(buffer, 0, (int)(remainingBytes % buffer.length));
          remainingBytes -= remainingBytes % buffer.length;
          // Write chunks of size buffer.length
          while (remainingBytes > 0) {
            cellStreams[i][j].write(buffer);
            remainingBytes -= buffer.length;
          }
          // Close stream
          cellStreams[i][j].close();
          // Append to the list of paths to concatenate
          pathsToConcat.add(getCellFilePath(i, j));
        }
      }
    }
    LOG.info("Output files stuffed with empty lines");

    Path outputFilePath = null;
    // Rename first file to target file name
    if (!pathsToConcat.isEmpty()) {
      Path firstFile = pathsToConcat.remove(0);
      outputFilePath = new Path(outputFilename);
      if (fs.exists(outputFilePath))
        fs.delete(outputFilePath, false);
      fs.rename(firstFile, outputFilePath);
    }

    // concatenate all files to one file
    if (!pathsToConcat.isEmpty()) {
      Path[] paths = pathsToConcat.toArray(new Path[pathsToConcat.size()]);
      ((DistributedFileSystem)fs).concat(outputFilePath, paths);
    }
    LOG.info("All intermediate files concatenated in one file");

    /*
		// Write a file for the histogram
		Path histFilepath = new Path(histogramFilename);
		if (fs.exists(histFilepath)) {
			// remove the file first
			fs.delete(histFilepath, false);
		}

		FSDataOutputStream os = fs.create(histFilepath);

		for (int j = 0; j < GridRows; j++) {
			for (int i = 0; i < GridColumns; i++) {
				System.out.print(histogram[i][j] + " ");
				os.writeInt(histogram[i][j]);
			}
			System.out.print("\n");
		}
		os.close();*/
	}
}