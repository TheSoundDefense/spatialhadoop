package edu.umn.cs.spatialHadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;


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

  public static void SpatialJoin_planeSweep(List<? extends TigerShape> R,
      List<? extends TigerShape> S, OutputCollector<TigerShape, TigerShape> output)
      throws IOException {
    
    LOG.info("Joining "+ R.size()+" with "+S.size());
    Collections.sort(R);
    Collections.sort(S);

		int i = 0, j = 0;

    while (i < R.size() && j < S.size()) {
      TigerShape r, s;
      if (R.get(i).compareTo(S.get(j)) < 0) {
        r = R.get(i);
        int jj = j;

        while ((jj < S.size())
            && ((s = S.get(jj)).getMBR().getX1() <= r.getMBR().getX2())) {
          if (r.isIntersected(s)) {
            output.collect(r, s);
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
            output.collect(r, s);
          }
          ii++;
        }
        j++;
      }
    }
	}
}
