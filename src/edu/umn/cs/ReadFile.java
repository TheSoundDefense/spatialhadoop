package edu.umn.cs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFile {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path inFile = new Path(args[0]);
    FileSystem fs = inFile.getFileSystem(conf);
    
    long length = fs.getFileStatus(inFile).getLen();
    
    BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fs.getFileStatus(inFile), 0, length);
    for (BlockLocation blk : fileBlockLocations) {
      System.out.println(blk);
      System.out.println(blk.getCellInfo());
    }
  }
}
