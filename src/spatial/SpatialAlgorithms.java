package spatial;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Performs simple algorithms for spatial data.
 * @author aseldawy
 *
 */
public class SpatialAlgorithms {
	
	public static Collection<PairOfRectangles> spatialJoin(Collection<Rectangle> rectangles) {
		if (rectangles.size() > 0)
			System.out.println("Doing spatial joing for " + rectangles.size() + " rectangles");
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
	
	public static void main(String[] args) {
		Collection<Rectangle> rectangles = new ArrayList<Rectangle>();
		Rectangle r1 = new Rectangle(1, 0,0, 100, 100);
		Rectangle r2 = new Rectangle(1, 0,0, 50, 50);
		Rectangle r3 = new Rectangle(1, -100,-100, -50, -50);
		Rectangle r4 = new Rectangle(1, -100,0, -50, 50);
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
