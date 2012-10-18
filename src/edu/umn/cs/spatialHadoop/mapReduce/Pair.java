package edu.umn.cs.spatialHadoop.mapReduce;


/**A class that stores a pair of object for spatial join*/
public class Pair<T> {
  public static final String Separator = "<--->";
  
  public T first;
  public T second;
  
  public Pair() {}
  
  public Pair(T first, T second) {
    this.first = first;
    this.second = second;
  }
  
  @Override
  public String toString() {
    return first.toString()+Separator+second.toString();
  }
}