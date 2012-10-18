package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;


public class PairOfFileSplits extends PairWritable<FileSplit> implements InputSplit {

  public PairOfFileSplits() {
    first = new FileSplit(new Path("/"), 0, 0, new String[] {});
    second = new FileSplit(new Path("/"), 0, 0, new String[] {});
  }
  
  public PairOfFileSplits(FileSplit first, FileSplit second) {
    super(first, second);
  }

  public long getLength() throws IOException {
    return first.getLength() + second.getLength();
  }

  public String[] getLocations() throws IOException {
    HashSet<String> combinedLocations = new HashSet<String>();
    for (String location : first.getLocations())
      combinedLocations.add(location);
    for (String location : second.getLocations())
      combinedLocations.add(location);
    return combinedLocations.toArray(new String[combinedLocations.size()]);
  }
}
