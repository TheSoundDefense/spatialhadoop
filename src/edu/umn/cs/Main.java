package edu.umn.cs;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.SpatialAlgorithms;

public class Main {

  /**
   * @param args
   */
  public static void main(String[] args) {
    long t1, t2;
    
    t1 = System.currentTimeMillis();
    Vector<Rectangle> R = new Vector<Rectangle>();
    Vector<Rectangle> S = new Vector<Rectangle>();
    for (int i = 0; i < 65536; i++) {
      Rectangle r = new Rectangle();
      r.x = (long) (Math.random() * 100000);
      r.y = (long) (Math.random() * 100000);
      r.width = (long) (Math.random() * 100);
      r.height = (long) (Math.random() * 100);
      R.add(r);

      Rectangle x = new Rectangle();
      x.x = (long) (Math.random() * 100000);
      x.y = (long) (Math.random() * 100000);
      x.width = (long) (Math.random() * 100);
      x.height = (long) (Math.random() * 100);
      S.add(x);
    }
    t2 = System.currentTimeMillis();
    
    System.out.println("Generated rectangles in "+(t2-t1)+" millis");
    
    long count;
    count = 0;
    try {
      t1 = System.currentTimeMillis();
      count = SpatialAlgorithms.SpatialJoin_planeSweep(R, S, null);
    } catch (IOException e) {
      e.printStackTrace();
    }
    t2 = System.currentTimeMillis();
    System.out.println("Found "+count+" results with planesweep in "+(t2-t1)+" millis");

    count = 0;
    int progress = 0;
    t1 = System.currentTimeMillis();
    for (Rectangle r : R) {
      progress++;
      if (progress % 1000 == 0) {
        System.out.println("Progress: "+(progress * 100 / R.size()));
      }
      for (Rectangle s : S) {
        if (r.isIntersected(s)) {
          count++;
        }
      }
    }
    t2 = System.currentTimeMillis();
    System.out.println("Found "+count+" results in "+(t2-t1)+" millis");
  }
}
