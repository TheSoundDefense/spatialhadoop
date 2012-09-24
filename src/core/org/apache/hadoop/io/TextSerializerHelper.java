package org.apache.hadoop.io;

public final class TextSerializerHelper {
  /**
   * All possible chars for representing a number as a String
   */
  final static byte[] digits = {
    '0' , '1' , '2' , '3' , '4' , '5' ,
    '6' , '7' , '8' , '9' , 'a' , 'b' ,
    'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
    'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
    'o' , 'p' , 'q' , 'r' , 's' , 't' ,
    'u' , 'v' , 'w' , 'x' , 'y' , 'z'
  };
  
  static byte[] buffer = new byte[64];

  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param i
   * @param t
   * @param appendComma
   */
  public static void serializeLong(long i, Text t, boolean appendComma) {
    int charPos = 64;
    if (appendComma) {
      buffer[--charPos] = ',';
    }
    final int shift = 4;
    final int radix = 1 << shift;
    final long mask = radix - 1;
    
    // Negative sign is prepended separately for negative numbers
    boolean negative = false;
    if (i < 0) {
      i = -i;
      negative = true;
    }
    do {
      buffer[--charPos] = digits[(int)(i & mask)];
      i >>>= shift;
    } while (i != 0);
    if (negative)
      buffer[--charPos] = '-';
    t.append(buffer, charPos, buffer.length - charPos);
  }
  
  /**
   * Parses only long from the given byte array (string). The long starts at
   * offset and is len characters long.
   * @param buf
   * @param offset
   * @param len
   * @return
   */
  public static long deserializeLong(byte[] buf, int offset, int len) {
    boolean negative = false;
    if (buf[offset] == '-') {
      negative = true;
      offset++;
      len--;
    }
    long i = 0;
    while (len-- > 0) {
      i <<= 4;
      if (buf[offset] <= '9')
        i |= buf[offset++] - '0';
      else
        i |= buf[offset++] - 'a' + 10;
    }
    return negative ? -i : i;
  }
}
