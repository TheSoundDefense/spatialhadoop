package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatial.Rectangle;

public class RectOutputFormat extends FileOutputFormat<Object, Rectangle> {
  public static final String OUTPUT_GRID = "edu.umn.cs.spatial.mapReduce.RectOutputFormat.GridInfo";
  private static final int BufferLength = 1024 * 1024;
  private static long BlockSize = 64 * 1024 * 1024;

  @Override
  public RecordWriter<Object, Rectangle> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress)
      throws IOException {
    // Output file name
    Path outFile = FileOutputFormat.getTaskOutputPath(job, name);
    
    // Get file system
    FileSystem fileSystem = outFile.getFileSystem(job);
    
    // Get grid info
    String grid = job.get(OUTPUT_GRID, "0,0,0,0,0,0");
    System.out.println("Writing accordding to the grid: "+grid);
    String[] parts = grid.split(",");
    GridInfo gridInfo = new GridInfo();
    gridInfo.xOrigin = Integer.parseInt(parts[0]);
    gridInfo.yOrigin = Integer.parseInt(parts[1]);
    gridInfo.gridWidth = Integer.parseInt(parts[2]);
    gridInfo.gridHeight = Integer.parseInt(parts[3]);
    gridInfo.cellWidth = Integer.parseInt(parts[4]);
    gridInfo.cellHeight = Integer.parseInt(parts[5]);
    return new RectRecordWriter(fileSystem, outFile, gridInfo);
  }

  protected static class RectRecordWriter implements RecordWriter<Object, Rectangle> {
    private final GridInfo gridInfo;
    /**An output stream for each grid cell*/
    private static PrintStream[][] cellStreams;
    private static long[][] cellSizes;
    private final Path outFile;
    private final FileSystem fileSystem;
    
    public RectRecordWriter(FileSystem fileSystem, Path outFile, GridInfo gridInfo) {
      this.fileSystem = fileSystem;
      this.outFile = outFile;
      this.gridInfo = gridInfo;
      
      // Prepare arrays that hold streams
      int gridColumns = (int) Math.ceil((double)gridInfo.gridWidth / gridInfo.cellWidth); 
      int gridRows = (int) Math.ceil((double)gridInfo.gridHeight / gridInfo.cellHeight);
      cellStreams = new PrintStream[gridColumns][gridRows];
      cellSizes = new long[gridColumns][gridRows];
    }

    public synchronized void write(Object cell, Rectangle rect) throws IOException {
      String line = rect.id+","+rect.x+","+rect.y+","+rect.width+","+rect.height;
      // Write to all possible grid cells
      for (long x = rect.getX1(); x < rect.getX2(); x += gridInfo.cellWidth) {
        for (long y = rect.getY1(); y < rect.getY2(); y += gridInfo.cellHeight) {
          PrintStream ps = getOrCreateCellFile(x, y);
          ps.println(line);
          // increase number of bytes written to this print stream
          int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
          int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
          cellSizes[column][row] += line.length() + 1;
        }
      }
    }

    public synchronized void close(Reporter reporter) throws IOException {
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

      // concatenate all files to one file
      if (!pathsToConcat.isEmpty()) {
        // Concatenate all files into the first file
        Path firstFile = pathsToConcat.remove(0);
        
        if (!pathsToConcat.isEmpty()) {
          Path[] paths = pathsToConcat.toArray(new Path[pathsToConcat.size()]);
          ((DistributedFileSystem)fileSystem).concat(firstFile, paths);
        }
      }
    }
    
    /**
     * Return path to a file that is used to write one grid cell
     * @param column
     * @param row
     * @return
     */
    private Path getCellFilePath(int column, int row) {
      return new Path(outFile.toUri().getPath() + '_' + column + '_' + row);
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
    public PrintStream getOrCreateCellFile(long x, long y) throws IOException {
      int column = (int)((x - gridInfo.xOrigin) / gridInfo.cellWidth);
      int row = (int)((y - gridInfo.yOrigin) / gridInfo.cellHeight);
      PrintStream ps = cellStreams[column][row];
      if (ps == null) {
        FSDataOutputStream os = fileSystem.create(getCellFilePath(column, row), gridInfo);
        cellStreams[column][row] = ps = new PrintStream(os);
        long xCell = column * gridInfo.cellWidth + gridInfo.xOrigin;
        long yCell = row * gridInfo.cellHeight + gridInfo.yOrigin;
        System.out.println("Setting next block at "+xCell+","+ yCell);
        ((DFSOutputStream)os.getWrappedStream()).setNextBlockCell(xCell, yCell);
      }
      return ps;
    }
  }
}
