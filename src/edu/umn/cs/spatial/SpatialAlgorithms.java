package edu.umn.cs.spatial;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;

import edu.umn.cs.spatial.mapReduce.CollectionWritable;


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

  public static void SpatialJoin_planeSweep(List<Rectangle> R,
      List<Rectangle> S, OutputCollector<Rectangle, Rectangle> output)
      throws IOException {
    Collections.sort(R);
    Collections.sort(S);

		int i = 0, j = 0;

    while (i < R.size() && j < S.size()) {
      Rectangle r, s;
      if (R.get(i).compareTo(S.get(j)) < 0) {
        r = R.get(i);
        int jj = j;

        while ((jj < S.size())
            && ((s = S.get(jj)).getX1() <= r.getX2())) {
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
            && ((r = R.get(ii)).getX1() <= s.getX2())) {
          if (r.isIntersected(s)) {
            output.collect(r, s);
          }
          ii++;
        }
        j++;
      }
    }
	}

	public static Collection<PairOfRectangles> spatialJoin(
			Collection<Rectangle> rectangles) {
		Collection<PairOfRectangles> matches = new ArrayList<PairOfRectangles>();

		// TODO use a sweep line algorithm instead of this quadratic algorithm
		for (Rectangle r1 : rectangles) {
			for (Rectangle r2 : rectangles) {
				if (r1 != r2 && r1.isIntersected(r2)) {
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
				if (range.isIntersected(r))
					matches.add(r);
			}
		} else {
			for (Rectangle r : rects) {
				if (condition == r.type)
					if (range.isIntersected(r))
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
				float dist = (r.getXMid() -s.getXMid())*  (r.getXMid() -s.getXMid()) +(r.getYMid() -s.getYMid())*(r.getYMid() -s.getYMid());
				topk.add(s, dist);
			}
			for (RectangleNN snn : topk.heap){
				result.add(new PairOfRectangles(r,snn.r));
			}
		}
		return result;
	}
	
	/**
	 * Finds the cells that need to be examined to find k-nearest neighbors
	 * to point p.
	 * @param p
	 * @param histogram
	 * @param gridOrigin
	 * @param cellDimensions
	 * @param k
	 * @return
	 */
	public static int[] KnnCells(Point p, int[][] histogram, double gridX1,
			double gridY1, double gridX2, double gridY2,
			double gridCellWidth, double gridCellHeight, int k) {
		int columns = (int)Math.ceil((gridX2 - gridX1) / gridCellWidth); 
		// TODO use the correct algorithm in paper
		// XXX for simplicity, we'll just add random grid cells
		int cell1x = (int)Math.floor((p.x - gridX1) / gridCellWidth);
		int cell1y = (int)Math.floor((p.y - gridY1) / gridCellHeight);
		int cell1 = cell1y * columns + cell1x;
		int cell2, cell3;
		cell2 = cell1x == 0 ? cell1 + 1 : cell1 - 1;
		cell3 = cell1y == 0 ? cell1 + columns : cell1 - columns;

		return new int[] {cell1, cell2, cell3};
	}

	public static int[][] readHistogram(JobConf job,String histogramFilename,
			int columns, int rows) throws IOException {
		final Path file = new Path(histogramFilename);
	    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
	    CompressionCodec codec = compressionCodecs.getCodec(file);
	    
	    // open the file and seek to the start of the split
	    final FileSystem fs = file.getFileSystem(job);
	    FSDataInputStream fileIn = fs.open(file);

	    DataInputStream in;
	    
		long start = 0;

		if (codec != null) {
			Decompressor decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn =
					((SplittableCompressionCodec)codec).createInputStream(
							fileIn, decompressor, 0, file.getFileSystem(job).getFileStatus(file).getLen(),
							SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = new DataInputStream(cIn);
				start = cIn.getAdjustedStart();
				long end = cIn.getAdjustedEnd();
			} else {
				in = new DataInputStream(codec.createInputStream(fileIn, decompressor));
			}
		} else {
			fileIn.seek(start);
			in = fileIn;
		}

		int[][] histogram = new int[rows][columns];
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < columns; j++) {
				histogram[i][j] = in.readInt();
			}
		}
		
		in.close();
	    return histogram;
	}
}
