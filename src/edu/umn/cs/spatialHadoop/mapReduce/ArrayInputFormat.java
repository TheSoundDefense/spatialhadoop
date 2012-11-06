package edu.umn.cs.spatialHadoop.mapReduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;

public abstract class ArrayInputFormat<S extends Shape>
    extends SpatialInputFormat<CellInfo, ArrayWritable> {

}
