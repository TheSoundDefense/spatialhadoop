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
  
  final static boolean[] HexadecimalChars;
  
  static byte[] buffer = new byte[64];
  
  static {
    HexadecimalChars = new boolean[256];
    for (char i = 'a'; i <= 'f'; i++)
      HexadecimalChars[i] = true;
    for (char i = 'A'; i <= 'F'; i++)
      HexadecimalChars[i] = true;
    for (char i = '0'; i <= '9'; i++)
      HexadecimalChars[i] = true;
    HexadecimalChars['-'] = true;
  }

  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param i
   * @param t
   * @param appendComma
   */
  public static void serializeLong(long i, Text t, char toAppend) {
    int charPos = 64;
    if (toAppend != '\0') {
      buffer[--charPos] = (byte)toAppend;
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
  
  /**
   * Deserializes and consumes a long from the given text. Consuming means all
   * characters read for deserialization are removed from the given text.
   * If separator is non-zero, a long is read and consumed up to the first
   * occurence of this separator. The separator is also consumed.
   * @param text
   * @param separator
   * @return
   */
  public static long consumeLong(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && HexadecimalChars[bytes[i]])
      i++;
    long l = deserializeLong(bytes, 0, i);
    // If the first char after the long is the separator, skip it
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    // Shift bytes after the long
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return l;
  }
  
  
  /**
   * Deserializes and consumes a double from the given text. Consuming means all
   * characters read for deserialization are removed from the given text.
   * If separator is non-zero, a double is read and consumed up to the first
   * occurence of this separator. The separator is also consumed.
   * @param text
   * @param separator
   * @return
   */
  public static double consumeDouble(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && bytes[i] != separator)
      i++;
    double d = Double.parseDouble(new String(bytes, 0, i));
    if (i < text.getLength())
      i++;
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return d;
  }
  
  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param i
   * @param t
   * @param appendComma
   */
  public static void serializeDouble(double d, Text t, char toAppend) {
    byte[] bytes = Double.toString(d).getBytes();
    t.append(bytes, 0, bytes.length);
  }
}
