package edu.umn.cs;

import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;

import edu.umn.cs.spatialHadoop.PointWithK;

public class CommandLineArguments {
  private String[] inputFilenames;
  private String[] outputFilenames;
  private GridInfo gridInfo;
  private Rectangle rectangle;
  private PointWithK pointWithK;
  private boolean rtree;
  private boolean pack;
  
  public Rectangle getRectangle() {
    return rectangle;
  }

  public CommandLineArguments(String[] args) {
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
}
