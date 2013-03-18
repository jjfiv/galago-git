// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 *
 * @author trevor
 */
public class StreamCreator {

  private static String stripPrefix(String filename) {
    String[] fields = filename.split(":");
    if (fields.length > 1) {
      return filename.substring(fields[0].length() + 1);
    }

    return filename;
  }

  public static InputStream bufferedInputStream(String filename) throws IOException {
    return new BufferedInputStream(new FileInputStream(filename));
  }
  
  public static FileInputStream realInputStream(String filename) throws IOException {
    FileInputStream stream = new FileInputStream(filename);
    return stream;
  }

  public static RandomAccessFile inputStream(String filename) throws IOException {
    RandomAccessFile file = new RandomAccessFile(filename, "r");
    return file;
  }

  public static RandomAccessFile outputStream(String filename) throws IOException {
    RandomAccessFile file = new RandomAccessFile(filename, "rw");
    return file;
  }

  public static DataOutputStream realOutputStream(String filename) throws IOException {
    DataOutputStream file = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
    return file;
  }

  public static DataInputStream openInputStream(String filename) throws IOException {
    if (filename.endsWith(".gz")) {
      return new DataInputStream(new GZIPInputStream(new FileInputStream(filename)));
    } else if (filename.endsWith(".bz")) {
      return new DataInputStream(new BZip2CompressorInputStream(new FileInputStream(filename)));
    } else {
      return new DataInputStream(new FileInputStream(filename));
    }
  }

  public static DataOutputStream openOutputStream(String filename) throws IOException {
    if (filename.endsWith(".gz")) {
      return new DataOutputStream(new GZIPOutputStream(new FileOutputStream(filename)));
    } else if (filename.endsWith(".bz")) {
      return new DataOutputStream(new BZip2CompressorOutputStream(new FileOutputStream(filename)));
    } else {
      return new DataOutputStream(new FileOutputStream(filename));
    }
  }
}
