
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author jfoley
 */
public class GalagoConf {
  private static final Logger LOG = Logger.getLogger(GalagoConf.class.getName());
 
  private static final Parameters preferences = Parameters.create();
  private static final Parameters memoryOptions = Parameters.create();
  private static final Parameters sorterOptions = Parameters.create();
  
  public static List<File> findGalagoConfigFiles() {
    ArrayList<File> files = new ArrayList<>();
    
    File homePrefs = new File(System.getProperty("user.home"), ".galago.conf");
    if (homePrefs.exists()) {
      files.add(homePrefs);
    }
    
    File curPrefs = new File(".galago.conf");
    if(curPrefs.exists()) {
      files.add(curPrefs);
    }
    
    return files;
  }
  
  /**
   * Put all initialization here
   */
  static {
    // try to find a prefs file
    try {
      
      for(File prefsFile : findGalagoConfigFiles()) {
        preferences.copyFrom(Parameters.parseFile(prefsFile));
      }
      
      if (preferences.containsKey("tmpdir")) {
        for (String tmp : preferences.getAsList("tmpdir", String.class)) {
          FileUtility.addTemporaryDirectory(tmp);
        }
      }
    
    } catch (IOException ioe) {
      LOG.warning("Unable to locate or read ~/.galago.conf or .galago.conf file. Using default settings, including tmpdir.\n" + ioe.getMessage());
    }

    if (preferences.containsKey("memory")) {
      memoryOptions.copyFrom(preferences.getMap("memory"));
    }

    if (preferences.containsKey("sorter")) {
      sorterOptions.copyFrom(preferences.getMap("sorter"));
    }
  }
  
  /**
   * @return All static parameters
   */
  public static Parameters getAllOptions() {
    return preferences;
  }

  /**
   * @return memory parameters
   *
   */
  public static Parameters threaded() {
    return memoryOptions;
  }

  /**
   * @return Sorter parameters
   */
  public static Parameters getSorterOptions() {
    return sorterOptions;
  }
  
  
}
