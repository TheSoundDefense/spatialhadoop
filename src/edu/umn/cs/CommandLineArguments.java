package edu.umn.cs;

import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;

import edu.umn.cs.spatialHadoop.PointWithK;

public class CommandLineArguments {
  private String[] args;
  private String[] inputFilenames;
  private String[] outputFilenames;
  private GridInfo gridInfo;
  private Rectangle rectangle;
  private PointWithK pointWithK;
  private boolean rtree;
  private boolean pack;
  private boolean overwrite;
  
  private static Set<String> Keys;
  private static Set<String> Options;
  
  static {
    Keys = new HashSet<String>();
    Keys.add("grid"); Keys.add("g");
    Keys.add("rectangle"); Keys.add("rect");
    Keys.add("point"); Keys.add("pt");
    Keys.add("size");
    
    Options = new HashSet<String>();
    Options.add("-rtree");
    Options.add("-pack");
    Options.add("-overwrite");
  }
  
  public Rectangle getRectangle() {
    return rectangle;
  }

  public CommandLineArguments(String[] args) {
    this.args = args;
    rtree = false;
    pack = false;
    Vector<String> paths = new Vector<String>();
    for (String arg : args) {
      if (arg.startsWith("grid:") || arg.startsWith("g:")) {
        gridInfo = new GridInfo();
        String strGrid = arg.substring(arg.indexOf(':') + 1);
        gridInfo.readFromString(strGrid);
      } else if (arg.startsWith("rectangle:") || arg.startsWith("rect:")) {
        rectangle = new Rectangle();
        String strRect = arg.substring(arg.indexOf(':') + 1);
        rectangle.readFromString(strRect);
      } else if (arg.startsWith("point:") || arg.startsWith("pt:")) {
        pointWithK = new PointWithK();
        String strPoint = arg.substring(arg.indexOf(':') + 1);
        pointWithK.readFromString(strPoint);
      } else if (arg.equals("-rtree")) {
        rtree = true;
      } else if (arg.equals("-pack")) {
        pack = true;
      } else if (arg.equals("-overwrite")) {
        overwrite = true;
      } else {
        paths.add(arg);
      }
    }
    outputFilenames = new String[1];
    inputFilenames = new String[paths.size() - 1];
    for (int i = 0; i < paths.size() - 1; i++) {
      inputFilenames[i] = paths.elementAt(i);
    }
    outputFilenames[0] = paths.elementAt(paths.size() - 1);
  }
  
  public Path[] getInputPaths() {
    Path[] inputPaths = new Path[inputFilenames.length];
    for (int i = 0; i < inputFilenames.length; i++) {
      inputPaths[i] = new Path(inputFilenames[i]);
    }
    return inputPaths;
  }
  
  public Path getInputPath() {
    return getInputPaths()[0];
  }
  
  public String[] getInputFilenames() {
    return inputFilenames;
  }
  
  public String getInputFilename() {
    return getInputFilenames()[0];
  }
  
  public Path[] getOutputPaths() {
    Path[] outputPaths = new Path[outputFilenames.length];
    for (int i = 0; i < outputFilenames.length; i++) {
      outputPaths[i] = new Path(outputFilenames[i]);
    }
    return outputPaths;
  }
  
  public Path getOutputPath() {
    return getOutputPaths()[0];
  }
  
  public String[] getOutputFilenames() {
    return outputFilenames;
  }
  
  public String getOutputFilename() {
    return getOutputFilenames()[0];
  }
  
  public GridInfo getGridInfo() {
    return gridInfo;
  }

  public PointWithK getPointWithK() {
    return pointWithK;
  }

  public boolean isRtree() {
    return rtree;
  }

  public boolean isPack() {
    return pack;
  }
  
  public boolean isOverwrite() {
    return overwrite;
  }
  
  public String getFilename() {
    for (String arg : args) {
      String possibleKey = arg.split(":", 2)[0];
      if (!Keys.contains(possibleKey) && !Options.contains(arg)) {
        // Everything left over is a file
        return arg;
      }
    }
    return null;
  }
  
  public Path getFilePath() {
    String filename = getFilename();
    return filename == null ? null : new Path(filename);
  }

  public long getSize() {
    for (String arg : args) {
      if (arg.startsWith("size:")) {
        return Long.parseLong(arg.split(":")[1]);
      }
    }
    return 0;
  }
}
