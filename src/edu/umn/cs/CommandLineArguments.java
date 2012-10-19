package edu.umn.cs;

import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;

import edu.umn.cs.spatialHadoop.PointWithK;

public class CommandLineArguments {
  private String[] args;

  public CommandLineArguments(String[] args) {
    this.args = args;
  }
  
  public Rectangle getRectangle() {
    Rectangle rect = null;
    for (String arg : args) {
      if (arg.startsWith("rect:") || arg.startsWith("rectangle:")) {
        rect = new Rectangle();
        rect.readFromString(arg.substring(arg.indexOf(':')+1));
      }
    }
    return rect;
  }
  
  public Path[] getPaths() {
    Vector<Path> inputPaths = new Vector<Path>();
    for (String arg : args) {
      if (arg.startsWith("-") && arg.length() > 1) {
        // Skip
      } else if (arg.indexOf(':') != -1 && arg.indexOf(":/") == -1) {
        // Skip
      } else {
        inputPaths.add(new Path(arg));
      }
    }
    return inputPaths.toArray(new Path[inputPaths.size()]);
  }
  
  public Path getPath() {
    return getPaths()[0];
  }
  
  public GridInfo getGridInfo() {
    GridInfo grid = null;
    for (String arg : args) {
      if (arg.startsWith("grid:")) {
        grid = new GridInfo();
        grid.readFromString(arg.substring(arg.indexOf(':')+1));
      }
    }
    return grid;
  }

  public PointWithK getPointWithK() {
    PointWithK point = null;
    for (String arg : args) {
      if (arg.startsWith("point:")) {
        point = new PointWithK();
        point.readFromString(arg.substring(arg.indexOf(':')+1));
      }
    }
    return point;
  }

  public boolean isRtree() {
    return is("rtree");
  }

  public boolean isPack() {
    return is("pack");
  }
  
  public boolean isOverwrite() {
    return is("overwrite");
  }
  
  public long getSize() {
    for (String arg : args) {
      if (arg.startsWith("size:")) {
        String size_str = arg.split(":")[1];
        if (size_str.indexOf('.') == -1)
          return Long.parseLong(size_str);
        String[] size_parts = size_str.split("\\.", 2);
        long size = Long.parseLong(size_parts[0]);
        size_parts[1] = size_parts[1].toLowerCase();
        if (size_parts[1].startsWith("k"))
          size *= 1024;
        else if (size_parts[1].startsWith("m"))
          size *= 1024 * 1024;
        else if (size_parts[1].startsWith("g"))
          size *= 1024 * 1024 * 1024;
        else if (size_parts[1].startsWith("t"))
          size *= 1024 * 1024 * 1024 * 1024;
        return size;
      }
    }
    return 0;
  }

  public boolean isRandom() {
    return is("random");
  }
  
  protected boolean is(String flag) {
    String expected_arg = "-"+flag;
    for (String arg : args) {
      if (arg.equals(expected_arg))
        return true;
    }
    return false;
  }

  public int getCount() {
    for (String arg : args) {
      if (arg.startsWith("count:")) {
        return Integer.parseInt(arg.substring(arg.indexOf(':')+1));
      }
    }
    return 1;
  }

  public float getSelectionRatio() {
    for (String arg : args) {
      if (arg.startsWith("ratio:")) {
        return Float.parseFloat(arg.substring(arg.indexOf(':')+1));
      }
    }
    return -1.0f;
  }

  public int getConcurrency() {
    for (String arg : args) {
      if (arg.startsWith("concurrency:")) {
        return Integer.parseInt(arg.substring(arg.indexOf(':')+1));
      }
    }
    return Integer.MAX_VALUE;
  }
}
