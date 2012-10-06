package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class PairOfFileSplits implements InputSplit {
  public FileSplit fileSplit1;
  public FileSplit fileSplit2;

  public PairOfFileSplits() {
    fileSplit1 = new FileSplit(new Path(""), 0, 0, new String[] {});
    fileSplit2 = new FileSplit(new Path(""), 0, 0, new String[] {});
  }
  
  public PairOfFileSplits(FileSplit fileSplit1, FileSplit fileSplit2) {
    this.fileSplit1 = fileSplit1;
    this.fileSplit2 = fileSplit2;
  }

  public void write(DataOutput out) throws IOException {
    fileSplit1.write(out);
    fileSplit2.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    fileSplit1.readFields(in);
    fileSplit2.readFields(in);
  }

  public long getLength() throws IOException {
    return fileSplit1.getLength() + fileSplit2.getLength();
  }

  public String[] getLocations() throws IOException {
    HashSet<String> combinedLocations = new HashSet<String>();
    for (String location : fileSplit1.getLocations())
      combinedLocations.add(location);
    for (String location : fileSplit2.getLocations())
      combinedLocations.add(location);
    return combinedLocations.toArray(new String[combinedLocations.size()]);
  }
}
