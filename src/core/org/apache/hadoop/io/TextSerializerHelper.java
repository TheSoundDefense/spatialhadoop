package org.apache.hadoop.io;

import java.util.Arrays;

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
  final static boolean[] DecimalChars;
  
  /**64 bytes to append to a string if necessary*/
  final static byte[] ToAppend = new byte[64];
  
  static {
    HexadecimalChars = new boolean[256];
    DecimalChars = new boolean[256];
    for (char i = 'a'; i <= 'f'; i++)
      HexadecimalChars[i] = true;
    for (char i = 'A'; i <= 'F'; i++)
      HexadecimalChars[i] = true;
    for (char i = '0'; i <= '9'; i++) {
      DecimalChars[i] = true;
      HexadecimalChars[i] = true;
    }
    HexadecimalChars['-'] = true;
    DecimalChars['-'] = true;
    
    Arrays.fill(ToAppend, (byte)' ');
  }
  
  /**
   * Appends hex representation of the given number to the given string.
   * If append is set to true, a comma is also appended to the text.
   * @param i
   * @param t
   * @param appendComma
   */
  public static void serializeHexLong(long i, Text t, char toAppend) {
    // Calculate number of bytes needed to serialize the given long
    int bytes_needed = 0;
    long temp;
    if (i < 0) {
      bytes_needed++; // An additional
      temp = -i;
    } else {
      temp = i;
    }
    do {
      bytes_needed += 1;
      temp >>>= 4;
    } while (temp != 0);
    
    if (toAppend != '\0')
      bytes_needed++;

    // Reserve the bytes needed in the text
    t.append(ToAppend, 0, bytes_needed);
    // Extract the underlying buffer array and fill it directly
    byte[] buffer = t.getBytes();
    // Position of the next character to write in the text
    int position = t.getLength() - 1;
    
    if (toAppend != '\0')
      buffer[position--] = (byte) toAppend;
    
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
      buffer[position--] = digits[(int)(i & mask)];
      i >>>= shift;
    } while (i != 0);
    if (negative)
      buffer[position--] = '-';
  }
  
  /**
   * Parses only long from the given byte array (string). The long starts at
   * offset and is len characters long.
   * @param buf
   * @param offset
   * @param len
   * @return
   */
  public static long deserializeHexLong(byte[] buf, int offset, int len) {
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
  public static long consumeHexLong(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && HexadecimalChars[bytes[i]])
      i++;
    long l = deserializeHexLong(bytes, 0, i);
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
    if (toAppend != '\0') {
      t.append(new byte[] {(byte)toAppend}, 0, 1);
    }
  }
  
  public static void serializeLong(long i, Text t, char toAppend) {
    // Calculate number of bytes needed to serialize the given long
    int bytes_needed = 0;
    long temp;
    if (i < 0) {
      bytes_needed++; // An additional
      temp = -i;
    } else {
      temp = i;
    }
    do {
      bytes_needed += 1;
      temp /= 10;
    } while (temp != 0);
    
    if (toAppend != '\0')
      bytes_needed++;

    // Reserve the bytes needed in the text
    t.append(ToAppend, 0, bytes_needed);
    // Extract the underlying buffer array and fill it directly
    byte[] buffer = t.getBytes();
    // Position of the next character to write in the text
    int position = t.getLength() - 1;
    
    if (toAppend != '\0')
      buffer[position--] = (byte) toAppend;
    
    // Negative sign is prepended separately for negative numbers
    boolean negative = false;
    if (i < 0) {
      i = -i;
      negative = true;
    }
    do {
      int digit = (int) (i % 10);
      buffer[position--] = digits[digit];
      i /= 10;
    } while (i != 0);
    if (negative)
      buffer[position--] = '-';
  }
  
  public static long deserializeLong(byte[] buf, int offset, int len) {
    boolean negative = false;
    if (buf[offset] == '-') {
      negative = true;
      offset++;
      len--;
    }
    long i = 0;
    while (len-- > 0) {
      i *= 10;
      i += buf[offset++] - '0';
    }
    return negative ? -i : i;
  }
  
  public static long consumeLong(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && DecimalChars[bytes[i]])
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
  
  public static void serializeInt(int i, Text t, char toAppend) {
    // Calculate number of bytes needed to serialize the given long
    int bytes_needed = 0;
    int temp;
    if (i < 0) {
      bytes_needed++; // An additional
      temp = -i;
    } else {
      temp = i;
    }
    do {
      bytes_needed += 1;
      temp /= 10;
    } while (temp != 0);
    
    if (toAppend != '\0')
      bytes_needed++;

    // Reserve the bytes needed in the text
    t.append(ToAppend, 0, bytes_needed);
    // Extract the underlying buffer array and fill it directly
    byte[] buffer = t.getBytes();
    // Position of the next character to write in the text
    int position = t.getLength() - 1;
    
    if (toAppend != '\0')
      buffer[position--] = (byte) toAppend;
    
    // Negative sign is prepended separately for negative numbers
    boolean negative = false;
    if (i < 0) {
      i = -i;
      negative = true;
    }
    do {
      int digit = i % 10;
      buffer[position--] = digits[digit];
      i /= 10;
    } while (i != 0);
    if (negative)
      buffer[position--] = '-';
  }
  
  public static int deserializeInt(byte[] buf, int offset, int len) {
    boolean negative = false;
    if (buf[offset] == '-') {
      negative = true;
      offset++;
      len--;
    }
    int i = 0;
    while (len-- > 0) {
      i *= 10;
      i += buf[offset++] - '0';
    }
    return negative ? -i : i;
  }
  
  public static int consumeInt(Text text, char separator) {
    int i = 0;
    byte[] bytes = text.getBytes();
    // Skip until the separator or end of text
    while (i < text.getLength() && DecimalChars[bytes[i]])
      i++;
    int l = deserializeInt(bytes, 0, i);
    // If the first char after the long is the separator, skip it
    if (i < text.getLength() && bytes[i] == separator)
      i++;
    // Shift bytes after the long
    System.arraycopy(bytes, i, bytes, 0, text.getLength() - i);
    text.set(bytes, 0, text.getLength() - i);
    return l;
  }
  
}
