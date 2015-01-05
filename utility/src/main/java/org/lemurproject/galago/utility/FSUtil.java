package org.lemurproject.galago.utility;

import java.io.File;
import java.io.IOException;

/**
 * @author jfoley
 */
public class FSUtil {
  /**
   * <p>If the parent directories for this file don't exist, this function
   * creates them.</p>
   *
   * <p>Often we want to create a file, but we don't yet know if the parent path
   * has been created yet. Call this function immediately before opening a file
   * for writing to make sure those directories have been created.</p>
   *
   * @param filename A filename that will soon be opened for writing.
   */
  public static File makeParentDirectories(File filename) {
    File parent = filename.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
    return filename;
  }

  public static String makeParentDirectories(String filename) {
    makeParentDirectories(new File(filename));
    return filename;
  }

  public static String getExtension(File file) {
    String fileName = file.getName();

    // now split the filename on '.'s
    String[] fields = fileName.split("\\.");

    // A filename needs to have a period to have an extension.
    if (fields.length <= 1) {
      return "";
    }

    String last = fields[fields.length - 1];
    String secondToLast = "";
    if(fields.length > 2) {
      secondToLast = fields[fields.length - 2];
    }

    String lastWithDot = "."+last;
    for(String ext : StreamCreator.compressionExtensions) {
      if (lastWithDot.equals(ext)) {
        return secondToLast;
      }
    }
    return last;
  }

  public static void deleteDirectory(File directory) throws IOException {
    if (directory.isDirectory()) {
      File[] files = directory.listFiles();
      if(files != null) {
        for (File sub : files) {
          if (sub.isDirectory()) {
            deleteDirectory(sub);
          } else {
            sub.delete();
          }
        }
      }
    }
    directory.delete();
  }

  /* We depend on jars that only work on 1.6, so it's okay to use this 1.6-only function. */
  public static long getFreeSpace(String pathname) throws IOException {
    return (new File(pathname)).getUsableSpace();
  }
}
