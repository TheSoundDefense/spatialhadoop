package edu.umn.cs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.spatial.GridInfo;

public class ReadFile {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    // Get input file information (Grid information)
    Path inputFile = new Path(args[0]);
    FileStatus fileStatus = inputFile.getFileSystem(conf).getFileStatus(inputFile);
    
    // Determine which blocks are needed
    GridInfo gridInfo = fileStatus.getGridInfo();
    System.out.println(gridInfo);
  }
}
