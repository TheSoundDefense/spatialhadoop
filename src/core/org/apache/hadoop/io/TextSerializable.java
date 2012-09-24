package org.apache.hadoop.io;

/**
 * Implementing this interface allows objects to be converted easily
 * to and from a string.
 * @author aseldawy
 *
 */
public interface TextSerializable {
  /**
   * Store current object as string in the given text appending text already there.
   * @param text
   */
  public void toText(Text text);
  
  /**
   * Retrieve information from the given text.
   * @param text
   */
  public void fromText(Text text);
  
  /**
   * Write to a string
   * @return
   */
  public String writeToString();
  
  /**
   * Read from string
   * @param s
   */
  public void readFromString(String s);
}
