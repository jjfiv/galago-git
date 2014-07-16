
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
      long freeSpace = FSUtil.getFreeSpace(root);

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
      FSUtil.deleteDirectory(f);
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

    StreamUtil.copyStreamToFile(resourceStream, tmp);
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

    FSUtil.makeParentDirectories(temporaryDir.getAbsolutePath());
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


  public static File[] safeListFiles(File root) {
    final String psa = "Galago's ls is having getting no results... If you're not on a distributed file system, this just means your directory is empty.";

    File[] subs = root.listFiles();
    int count = 0;
    while (subs == null && count < 100) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) { }
      LOG.warning(psa);
      count++;
      subs = root.listFiles();
    }

    if (subs == null) {
      throw new IllegalStateException(psa);
    }
    return subs;
  }

  public static List<String> getRoots() {
    return roots;
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
   * @deprecated see FSUtil instead
   */
  @Deprecated
  public static void makeParentDirectories(File filename) {
    FSUtil.makeParentDirectories(filename);
  }

  /** @deprecated see FSUtil instead */
  @Deprecated
  public static void makeParentDirectories(String filename) {
    FSUtil.makeParentDirectories(new File(filename));
  }

  /** @deprecated see FSUtil instead */
  @Deprecated
  public static String getExtension(File file) {
    return FSUtil.getExtension(file);
  }

  /** @deprecated see FSUtil instead */
  @Deprecated
  public static long getFreeSpace(String pathname) throws IOException {
    return FSUtil.getFreeSpace(pathname);
  }
}
