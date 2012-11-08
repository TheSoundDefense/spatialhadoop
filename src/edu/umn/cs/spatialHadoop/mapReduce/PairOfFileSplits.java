package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;


public class PairOfFileSplits extends PairWritable<FileSplit> implements InputSplit {

  /**Locations of this split ordered by coverage of the underlying file parts*/
  protected String[] locations;
  
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
    if (locations == null) {
      Map<String, Integer> locationFrequency = new HashMap<String, Integer>();
      Vector<String> vLocations = new Vector<String>();
      for (String location : first.getLocations()) {
        if (!vLocations.contains(location))
          vLocations.add(location);
        if (locationFrequency.containsKey(location)) {
          locationFrequency.put(location, locationFrequency.get(location) + 1);
        } else {
          locationFrequency.put(location, 1);
        }
      }
      for (String location : second.getLocations()) {
        if (!vLocations.contains(location))
          vLocations.add(location);
        if (locationFrequency.containsKey(location)) {
          locationFrequency.put(location, locationFrequency.get(location) + 1);
        } else {
          locationFrequency.put(location, 1);
        }
      }
      // Order by frequency to move location with may frequencies to the front
      for (int i = 0; i < vLocations.size(); i++) {
        for (int j = 0; j < vLocations.size() - i - 1; j++) {
          if (locationFrequency.get(vLocations.elementAt(j)) <
              locationFrequency.get(vLocations.elementAt(j+1))) {
            String temp = vLocations.elementAt(j);
            vLocations.set(j, vLocations.elementAt(j+1));
            vLocations.set(j+1, temp);
          }
        }
      }
      locations = vLocations.toArray(new String[vLocations.size()]);
    }
    return locations;
  }
}
