package org.apache.hadoop.io;

/**
 * A modified version of Text which is optimized for appends.
 * @author eldawy
 *
 */
public class Text2 extends Text implements TextSerializable {

  public Text2() {
  }

  public Text2(String string) {
    super(string);
  }

  public Text2(Text utf8) {
    super(utf8);
  }

  public Text2(byte[] utf8) {
    super(utf8);
  }

  @Override
  protected void setCapacity(int len, boolean keepData) {
    if (bytes == null || bytes.length < len) {
      // Get the next power of two for len
      len--;
      len |= len >> 1;
      len |= len >> 2;
      len |= len >> 4;
      len |= len >> 8;
      len |= len >> 16;
      len |= len >> 32;
      len++;
      super.setCapacity(len, keepData);
    }
  }
  
  public void shrink(int newlen) {
    if (newlen < length) {
      length = newlen;
    }
  }

  @Override
  public Text toText(Text text) {
    text.append(getBytes(), 0, getLength());
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.set(text);
  }
}
