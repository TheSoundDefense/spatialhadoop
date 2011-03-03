package spatial;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import spatial.mapReduce.CollectionWritable;

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
		@Override
		public int compareTo(RectangleNN rect2) {
				RectangleNN r = (RectangleNN)rect2;
				float difference = this.dist - r.dist;
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
				RectangleNN removed = this.heap.pollFirst();
			}

		}
	}

public class SpatialAlgorithms {

	public static Collection<PairOfRectangles> SpatialJoin_planeSweep(
			CollectionWritable<CollectionWritable<Rectangle>> rectanglesLists) {
		Collection<PairOfRectangles> result = new ArrayList<PairOfRectangles>();
		ArrayList<Rectangle> R = new ArrayList<Rectangle>();
		ArrayList<Rectangle> S = new ArrayList<Rectangle>();
		int l = 0;
		for (CollectionWritable<Rectangle> rectangles : rectanglesLists) {
			if (l == 0) {
				for (Rectangle r : rectangles) {
					R.add(r);
				}
			} else {
				for (Rectangle s : rectangles) {
					S.add(s);
				}
			}
		}
		Collections.sort(R);
		Collections.sort(S);

		while (R.size() != 0 && S.size() != 0) {
			Rectangle r = null;
			if (R.get(0).compareTo(S.get(0)) < 0) {
				r = R.get(0);
				int i = 0;
				Rectangle s;
				while ((i < S.size())
						&& (S.get(i).getXlower() <= r.getXupper())) {
					s = S.get(i);
					if (r.intersects(s)) {
						result.add(new PairOfRectangles(r, s));
					}
					i++;
				}
				R.remove(0);
			} else {
				r = S.get(0);
				int i = 0;
				Rectangle s;
				while ((i < R.size())
						&& (R.get(i).getXlower() <= r.getXupper())) {
					s = R.get(i);
					if (r.intersects(s)) {
						result.add(new PairOfRectangles(s, r));
					}
					i++;
				}
				S.remove(0);
			}
		}
		return result;
	}

	public static Collection<PairOfRectangles> spatialJoin(
			Collection<Rectangle> rectangles) {
		if (rectangles.size() > 0)
			System.out.println("Doing spatial joing for " + rectangles.size()
					+ " rectangles");
		Collection<PairOfRectangles> matches = new ArrayList<PairOfRectangles>();

		// TODO use a sweep line algorithm instead of this quadratic algorithm
		for (Rectangle r1 : rectangles) {
			for (Rectangle r2 : rectangles) {
				if (r1 != r2 && r1.intersects(r2)) {
					matches.add(new PairOfRectangles(r1, r2));
				}
			}
		}

		return matches;
	}

	public static Collection<Rectangle> Range(Rectangle range,
			Collection<Rectangle> rects, int condition) {
		Collection<Rectangle> matches = new ArrayList<Rectangle>();
		if (condition == -1) {
			for (Rectangle r : rects) {
				if (range.intersects(r))
					matches.add(r);
			}
		} else {
			for (Rectangle r : rects) {
				if (condition == r.type)
					if (range.intersects(r))
						matches.add(r);
			}
		}
		return matches;
	}
    
	public static Collection<PairOfRectangles> NN(CollectionWritable<CollectionWritable<Rectangle>> rectanglesLists, int k) {
		Collection<PairOfRectangles> result = new ArrayList<PairOfRectangles>();
		ArrayList<Rectangle> R = new ArrayList<Rectangle>();
		ArrayList<Rectangle> S = new ArrayList<Rectangle>();
		int l = 0;
		for (CollectionWritable<Rectangle> rectangles : rectanglesLists) {
			if (l == 0) {
				for (Rectangle r : rectangles) {
					R.add(r);
				}
			} else {
				for (Rectangle s : rectangles) {
					S.add(s);
				}
			}
		}
		//Compute the NN
		for (Rectangle r : R) {
			TOPK topk = new TOPK(k);
			for (Rectangle s : S) {
				float dist = (r.midX() -s.midX())*  (r.midX() -s.midX()) +(r.midY() -s.midY())*(r.midY() -s.midY());
				topk.add(s, dist);
			}
			for (RectangleNN snn : topk.heap){
				result.add(new PairOfRectangles(r,snn.r));
			}
		}
		return result;
	}
	public static void main(String[] args) {
		Collection<Rectangle> rectangles = new ArrayList<Rectangle>();
		Rectangle r1 = new Rectangle(1, 0, 0, 100, 100);
		Rectangle r2 = new Rectangle(1, 0, 0, 50, 50);
		Rectangle r3 = new Rectangle(1, -100, -100, -50, -50);
		Rectangle r4 = new Rectangle(1, -100, 0, -50, 50);
		rectangles.add(r1);
		rectangles.add(r2);
		rectangles.add(r3);
		rectangles.add(r4);

		Collection<PairOfRectangles> results = spatialJoin(rectangles);
		if (!results.contains(new PairOfRectangles(r1, r2)))
			System.err.println("error1");
		if (results.contains(new PairOfRectangles(r1, r3)))
			System.err.println("error2");
		if (results.contains(new PairOfRectangles(r1, r4)))
			System.err.println("error3");
	}
}
