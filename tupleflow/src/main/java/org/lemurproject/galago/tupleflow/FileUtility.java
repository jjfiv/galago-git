
package org.lemurproject.galago.tupleflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author jfoley
 */
public class FileUtility {
  private static final Logger LOG = Logger.getLogger(FileUtility.class.getName());
  private static final List<String> roots = new ArrayList();  

  
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
  
  
  public static long getFreeSpace(String pathname) throws IOException {
    try {
      // this will only work in Java 1.6 or later
      Method m = File.class.getMethod("getUsableSpace");
      Long result = (Long) m.invoke(new File(pathname));
      return (long) result;
    } catch (Exception e) {
      try {
        return getUnixFreeSpace(pathname);
      } catch (Exception ex) {
        return 1024 * 1024 * 1024; // 1GB
      }
    }
  }

  public static long getUnixFreeSpace(String pathname) throws IOException {
    try {
      // BUGBUG: will not work on windows
      String[] command = {"df", "-Pk", pathname};
      Process process = Runtime.getRuntime().exec(command);
      InputStream procOutput = process.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(procOutput));

      // skip the first line
      reader.readLine();
      String line = reader.readLine();
      String[] fields = line.split("\\s+");
      reader.close();

      process.getErrorStream().close();
      process.getInputStream().close();
      process.getOutputStream().close();
      process.waitFor();

      long freeSpace = Long.parseLong(fields[3]) * 1024;
      return freeSpace;
    } catch (InterruptedException ex) {
      return 0;
    }
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

  public static void makeParentDirectories(String filename) {
    makeParentDirectories(new File(filename));
  }
}
