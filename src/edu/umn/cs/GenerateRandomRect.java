package edu.umn.cs;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class GenerateRandomRect {

  /**
   * # USAGE: generate_rectangles <output path> <file size> <type>
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    int x1 = 0;
    int y1 = 10;
    int x2 = 102400;
    int y2 = 102400;
    int maxWidth = 10;
    int maxHeight = 10;
    
    String outputFilename = "test.rect";
    if (args.length > 0)
      outputFilename = args[0];
    
    long totalSize = 1024*1024*1024;
    if (args.length > 1)
      totalSize = Long.parseLong(args[1]);
    
    DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFilename));
    Random random = new Random();
    long id = 1;
    
    while (totalSize > 0) {
      int w = random.nextInt(maxWidth);
      int h = random.nextInt(maxHeight);
      int x = random.nextInt((x2-x1) - w) + x1;
      int y = random.nextInt((y2-y1) - h) + y1;
      
      dos.writeLong(id++);
      dos.writeInt(x);
      dos.writeInt(y);
      dos.writeInt(x+w);
      dos.writeInt(y+h);
      dos.writeInt(5);
      
      totalSize -= 4 * 5 + 8;
    }
    
    dos.close();
  }

}
