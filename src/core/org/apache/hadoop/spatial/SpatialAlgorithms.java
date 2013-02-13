package org.apache.hadoop.spatial;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Performs simple algorithms for spatial data.
 * 
 * @author aseldawy
 * 
 */
class RectangleNN implements Comparable<RectangleNN>   {
	Rectangle r;
	float dist;
	public RectangleNN(Rectangle r, float dist){
		this.r =r ;
		this.dist =dist;	   
	}

	public int compareTo(RectangleNN rect2) {
		float difference = this.dist - rect2.dist;
		if (difference < 0) {
			return -1;
		} 
		if (difference > 0) {
			return 1;
		}
		return 0;

	}

}
class TOPK {
	public TreeSet<RectangleNN> heap;
	public int k;

	public TOPK(int k) {
		heap = new TreeSet<RectangleNN>();
		this.k = k;
	}

	public void add(Rectangle r,float dist) {
		heap.add(new RectangleNN(r, dist));
		if (this.heap.size() > k) {
			// Remove largest element in set (to keep it of size k)
			this.heap.last();
		}

	}
}

public class SpatialAlgorithms {
  public static final Log LOG = LogFactory.getLog(SpatialAlgorithms.class);
  
  /**
   * This method is deprectated. Use the array version.
   * @param R
   * @param S
   * @param output
   * @return
   * @throws IOException
   */
  @Deprecated
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      List<S1> R, List<S2> S, ResultCollector2<S1, S2> output)
      throws IOException {
    int count = 0;

    Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
        return o1.getMBR().x < o2.getMBR().x ? -1 : 1;
      }
    };

    LOG.info("Joining "+ R.size()+" with "+S.size());
    Collections.sort(R, comparator);
    Collections.sort(S, comparator);

		int i = 0, j = 0;

    while (i < R.size() && j < S.size()) {
      S1 r;
      S2 s;
      if (comparator.compare(R.get(i), S.get(j)) < 0) {
        r = R.get(i);
        int jj = j;

        while ((jj < S.size())
            && ((s = S.get(jj)).getMBR().getX1() <= r.getMBR().getX2())) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          jj++;
        }
        i++;
      } else {
        s = S.get(j);
        int ii = i;

        while ((ii < R.size())
            && ((r = R.get(ii)).getMBR().getX1() <= s.getMBR().getX2())) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          ii++;
        }
        j++;
      }
    }
    LOG.info("Finished plane sweep and found "+count+" pairs");
    return count;
	}

  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      final S1[] R, final S2[] S, ResultCollector2<S1, S2> output) {
    int count = 0;

    final Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
        return o1.getMBR().x < o2.getMBR().x ? -1 : 1;
      }
    };
    
    LOG.info("Joining "+ R.length+" with "+S.length);
    Arrays.sort(R, comparator);
    Arrays.sort(S, comparator);

    int i = 0, j = 0;

    while (i < R.length && j < S.length) {
      S1 r;
      S2 s;
      if (comparator.compare(R[i], S[j]) < 0) {
        r = R[i];
        int jj = j;

        while ((jj < S.length)
            && ((s = S[jj]).getMBR().getX1() <= r.getMBR().getX2())) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          jj++;
        }
        i++;
      } else {
        s = S[j];
        int ii = i;

        while ((ii < R.length)
            && ((r = R[ii]).getMBR().getX1() <= s.getMBR().getX2())) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          ii++;
        }
        j++;
      }
    }
    LOG.info("Finished plane sweep and found "+count+" pairs");
    return count;
  }
}
