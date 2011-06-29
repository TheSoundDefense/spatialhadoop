package edu.umn.cs;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.spatial.GridInfo;

public class WriteRectFile {

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
	public static PrintStream getOrCreateCellFile(int x, int y) throws IOException {
    int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
    int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
	  PrintStream ps = cellStreams[column][row];
	  if (ps == null) {
	    
	    FSDataOutputStream os = fs.create(getCellFilePath(column, row), gridInfo);
	    cellStreams[column][row] = ps = new PrintStream(os);
      int xCell = column * gridInfo.cellWidth + gridInfo.xOrigin;
      int yCell = row * gridInfo.cellHeight + gridInfo.yOrigin;
      System.out.println("Setting next block at "+xCell+","+ yCell);
	    ((DFSOutputStream)os.getWrappedStream()).setNextBlockCell(xCell, yCell);
	  }
	  return ps;
	}

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

    gridInfo.xOrigin = Integer.parseInt(parts[0]);
    gridInfo.yOrigin = Integer.parseInt(parts[1]);
    gridInfo.gridWidth = Integer.parseInt(parts[2]);
    gridInfo.gridHeight = Integer.parseInt(parts[3]);
    gridInfo.cellWidth = Integer.parseInt(parts[4]);
    gridInfo.cellHeight = Integer.parseInt(parts[5]);
		
		int gridColumns = (int) Math.ceil((double)gridInfo.gridWidth / gridInfo.cellWidth); 
    int gridRows = (int) Math.ceil((double)gridInfo.gridHeight / gridInfo.cellHeight);
    System.out.println("Grid "+gridColumns+"x"+gridRows);
    
    // Prepare an array to hold all output streams
    cellStreams = new PrintStream[gridColumns][gridRows];
    cellSizes = new long[gridColumns][gridRows];

    // Open input file
    LineNumberReader reader = new LineNumberReader(new FileReader(inputFilename));

    while (reader.ready()) {
      // TODO we can make it faster by not doing a trim or split to avoid creating unnecessary objects
      String line = reader.readLine().trim();
      // Parse rectangle dimensions
      parts = line.split(",");
      //int id = Integer.parseInt(parts[0]);
      int rx1 = Integer.parseInt(parts[1]);
      int ry1 = Integer.parseInt(parts[2]);
      int rx2 = Integer.parseInt(parts[3]);
      int ry2 = Integer.parseInt(parts[4]);

      // Write to all possible grid cells
      for (int x = rx1; x < rx2; x += gridInfo.cellWidth) {
        for (int y = ry1; y < ry2; y += gridInfo.cellHeight) {
          PrintStream ps = getOrCreateCellFile(x, y);
          ps.println(line);
          // increase number of bytes written to this print stream
          int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
          int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
          cellSizes[column][row] += line.length() + 1;
        }
      }
    }
    
    // Close input file
    reader.close();
    
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

    Path outputFilePath = null;
    // Rename first file to target file name
    if (!pathsToConcat.isEmpty()) {
      Path firstFile = pathsToConcat.remove(0);
      outputFilePath = new Path(outputFilename);
      fs.rename(firstFile, outputFilePath);
    }

    // concatenate all files to one file
    if (!pathsToConcat.isEmpty()) {
      Path[] paths = pathsToConcat.toArray(new Path[pathsToConcat.size()]);
      ((DistributedFileSystem)fs).concat(outputFilePath, paths);
    }

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