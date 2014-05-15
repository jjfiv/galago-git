
package org.lemurproject.galago.tupleflow;

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
 
  private static final Parameters preferences = new Parameters();
  private static final Parameters drmaaOptions = new Parameters();
  private static final Parameters sorterOptions = new Parameters();
  
  public static List<File> findGalagoConfigFiles() {
    ArrayList<File> files = new ArrayList<File>();
    
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

    if (preferences.containsKey("drmaa")) {
      drmaaOptions.copyFrom(preferences.getMap("drmaa"));
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
   * @return Drmaa parameters
   *
   */
  public static Parameters getDrmaaOptions() {
    return drmaaOptions;
  }

  /**
   * @return Sorter parameters
   */
  public static Parameters getSorterOptions() {
    return sorterOptions;
  }
  
  
}
