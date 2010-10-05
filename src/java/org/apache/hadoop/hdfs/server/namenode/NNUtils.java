package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;


/**
 * Utility class 
 * 
 */

public class NNUtils {

  static private final DeprecatedUTF8 U_STR = new DeprecatedUTF8();

  /**
   * 
   * @param u
   * @throws IOException
   */
  static public void checkSchemeConsistency(URI u) throws IOException {
    String scheme = u.getScheme();
    // the URI should have a proper scheme
    if(scheme == null)
        throw new IOException("Undefined scheme for " + u);
    else {
      try {
        // the scheme should be enumerated as JournalType
            JournalType.valueOf(scheme.toUpperCase());
        }catch (IllegalArgumentException iae){
          throw new IOException("Unknown scheme " + scheme + 
                ". It should correspond to a JournalType enumeration value");
        }
    }
  };
  
  // This should be reverted to package private once the ImageLoader
  // code is moved into this package. This method should not be called
  // by other code.
  static public  String readString(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    return U_STR.toString();
  }

  static public String readString_EmptyAsNull(DataInputStream in) throws IOException {
    final String s = readString(in);
    return s.isEmpty() ? null : s;
  }
  
  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * 
   * @param in
   * @return the array each element of which is a byte[] representation of a
   *         path component
   * @throws IOException
   */
  public static byte[][] readPathComponents(DataInputStream in)
      throws IOException {
    U_STR.readFields(in);
    return DFSUtil.bytes2byteArray(U_STR.getBytes(), U_STR.getLength(),
        (byte) Path.SEPARATOR_CHAR);

  }

  // Same comments apply for this method as for readString()
  public static byte[] readBytes(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    int len = U_STR.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(U_STR.getBytes(), 0, bytes, 0, len);
    return bytes;
  }

  static void writeString(String str, DataOutputStream out)
      throws IOException {
    U_STR.set(str);
    U_STR.write(out);
  }
  
  
  
}
