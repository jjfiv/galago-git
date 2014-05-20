
package org.lemurproject.galago.tupleflow;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author jfoley
 */
public class FileUtility {
  private static final Logger LOG = Logger.getLogger(FileUtility.class.getName());
  private static final List<String> roots = new ArrayList<String>();

  
  // dynamically add to the set of roots
  public static void addTemporaryDirectory(String path) {
    File f = new File(path);
    if (!f.isDirectory()) {
      f.mkdirs();
    }
    roots.add(path);
  }

  public static String getBestTemporaryLocation(long requiredSpace) throws IOException {
    for (String root : roots) {
      long freeSpace = getFreeSpace(root);

      if (freeSpace >= requiredSpace) {
        //String logString = String.format("Found %6.3fMB >= %6.3fMB left on %s",
        //        freeSpace / 1048576.0, requiredSpace / 1048576.0, root);
        //LOG.info(logString);
        return root;
      }
    }
    return null;
  }

  /**
   * remove all data from all temp directories - be very careful when using this
   * function!
   *
   * @throws IOException
   */
  public static void cleanTemporaryDirectories() throws IOException {
    for (String root : roots) {
      File f = new File(root);
      Utility.deleteDirectory(f);
      f.mkdir();
    }
  }
  
  // A workaround to make File versions of packaged resources. If it exists already, we return that and hope
  // it's what they wanted.
  // Note that we simply use the filename of the resource because, well, sometimes that's important when
  // poor coding is involved.
  public static File createResourceFile(Class requestingClass, String resourcePath) throws IOException {
    String tmpPath = getBestTemporaryLocation(1024 * 1024 * 100);
    if (tmpPath == null) {
      tmpPath = "";
    }

    String[] parts = resourcePath.split(File.separator);
    String fileName = parts[parts.length - 1];

    LOG.info(String.format("Creating resource file: %s/%s", tmpPath, fileName));
    File tmp = new File(tmpPath, fileName);
    if (tmp.exists()) {
      return tmp;
    }

    InputStream resourceStream = requestingClass.getResourceAsStream(resourcePath);
    if (resourceStream == null) {
      LOG.warning(String.format("Unable to create resource file."));
      return null;
    }

    Utility.copyStreamToFile(resourceStream, tmp);
    return tmp;
  }
  
  
  public static File createTemporaryDirectory() throws IOException {
    return createTemporaryDirectory("");
  }

  public static File createTemporaryDirectory(String path) throws IOException {
    File temporaryDir;
    if (path.length() > 0) {
      temporaryDir = new File(path);
    } else {
      temporaryDir = createTemporary();
    }

    makeParentDirectories(temporaryDir.getAbsolutePath());
    if (temporaryDir.isFile()) {
      temporaryDir.delete();
    }
    temporaryDir.mkdir();

    return temporaryDir;
  }

  public static File createTemporary() throws IOException {
    return createTemporary(1024 * 1024 * 1024);
  }

  public static File createTemporary(long requiredSpace) throws IOException {
    File temporary;
    String root = getBestTemporaryLocation(requiredSpace);
    if (root != null) {
      temporary = File.createTempFile("tupleflow", "", new File(root));
    } else {
      temporary = File.createTempFile("tupleflow", "");
    }

    // LOG.info("UTILITY_CREATED: " + temporary.getAbsolutePath());
    return temporary;
  }


  /* We depend on jars that only work on 1.6, so it's okay to use this 1.6-only function. */
  public static long getFreeSpace(String pathname) throws IOException {
    return (new File(pathname)).getUsableSpace();
  }

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
  public static void makeParentDirectories(File filename) {
    File parent = filename.getParentFile();
    if (parent != null) {
      parent.mkdirs();
    }
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

  public static File[] safeListFiles(File root) {
    File[] subs = root.listFiles();
    int count = 0;
    while (subs == null && count < 100) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) { }
      System.out.println("Listing files is taking a long time...");
      count++;
      subs = root.listFiles();
    }

    if (subs == null) {
      throw new IllegalStateException("safeListFiles is having a hard time with hte really slow filesystem :(... ");
    }
    return subs;
  }

  public static void makeParentDirectories(String filename) {
    makeParentDirectories(new File(filename));
  }

  public static List<String> getRoots() {
    return roots;
  }
}
