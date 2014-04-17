// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author trevor
 */
public class StreamCreator {

  public static InputStream bufferedInputStream(String filename) throws IOException {
    return new BufferedInputStream(new FileInputStream(filename));
  }
  
  public static FileInputStream realInputStream(String filename) throws IOException {
    FileInputStream stream = new FileInputStream(filename);
    return stream;
  }

  public static RandomAccessFile readFile(String filename) throws IOException {
    RandomAccessFile file = new RandomAccessFile(filename, "r");
    return file;
  }

  public static RandomAccessFile writeFile(String filename) throws IOException {
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
    } else if (filename.endsWith(".bz") || filename.endsWith(".bz2")) {
      return new DataInputStream(new BZip2CompressorInputStream(new FileInputStream(filename)));
    } else if(filename.endsWith(".xz")) {
      return new DataInputStream(new XZInputStream(new FileInputStream(filename)));
    } else {
      return new DataInputStream(new FileInputStream(filename));
    }
  }

  public static DataInputStream openInputStream(File fp) throws IOException {
    return openInputStream(fp.getAbsolutePath());
  }

  public static DataOutputStream openOutputStream(String filename) throws IOException {
    if (filename.endsWith(".gz")) {
      return new DataOutputStream(new GZIPOutputStream(new FileOutputStream(filename)));
    } else if (filename.endsWith(".bz") || filename.endsWith(".bz2")) {
      return new DataOutputStream(new BZip2CompressorOutputStream(new FileOutputStream(filename)));
    } else if(filename.endsWith(".xz")) {
      return new DataOutputStream(new XZOutputStream(new FileOutputStream(filename), new LZMA2Options()));
    } else {
      return new DataOutputStream(new FileOutputStream(filename));
    }
  }

  public static DataOutputStream openOutputStream(File path) throws IOException {
    return openOutputStream(path.getAbsolutePath());
  }

  public static String[] compressionExtensions = {".gz", ".bz", ".bz2", ".xz"};

  public static boolean isCompressed(String filename) {
    for(String ext : compressionExtensions) {
      if(filename.endsWith(ext))
        return true;
    }
    return false;
  }
}
