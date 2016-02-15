// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.util.Properties;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.InputStream;
import java.io.IOException;


/**
 * @author smh
 *
 * <p>Define galago version and build datetime strings as well as
 * the index build datetime (which is now()).</p>
 *
 * <p>The version and version build datetime stringsare derived
 * from core/src/main/resources/version.properties and is accessed
 * as the properties template file core/target/classes/version.properties.
 * The Maven core pom.xml automatically fills in the version and build
 * datetime strings when building the galago project.</p>
 */

public class VersionInfo {

  private static String galagoVersion = null;
  private static String buildDateTime = null;
  private static String actionDateTime = null;

  //- No instantiation needed.
  private VersionInfo() {
  }


  /**
   * Set galago version and build datetime strings by reading the
   * auto-generated version.properties file.  Also set the index
   * build datetime [now()].
   * @return void
   */
  public static void setGalagoVersionBuildAndIndexDateTime () {

    Properties props = new Properties ();

    Class versionClass = new Object() {}.getClass().getEnclosingClass();
    try (InputStream propFile = versionClass.getClass().getResourceAsStream ("/version.properties")) {
      props.load (propFile);

      if (props.containsKey ("version")) {
        galagoVersion = props.getProperty ("version");
      }
      else {
        galagoVersion = "Unknown";
      }

      if (props.containsKey ("build.date")) {
        buildDateTime = props.getProperty ("build.date");
      }
      else {
        buildDateTime = "Unknown";
      }
    }
    catch (IOException ioe) {
      System.out.println ("IO error reading properties\n" + ioe.toString());
    }

    //propFile.close ();

    //- Determine current galago function (index) run datetime  [now()]
    actionDateTime = LocalDateTime.now ().format (
                      DateTimeFormatter.ofPattern ("yyyy-MM-dd HH:mm"));

  }  //- end setGalagoVersionBuildAndIndexDateTime


  /**
   * Get galago version string
   * @return String
   */
  public static String getGalagoVersion() {
    return galagoVersion;
  }  //- end getGalagoVersion


  /**
   * Get Galago version build datetime
   * @return String
   */
  public static String getGalagoVersionBuildDateTime () {
    return buildDateTime;

  }  //- end getGalagoVersionBuildDateTime


  /**
   * Get Galago function action datetime
   * @return String
   */
  public static String getGalagoActionDateTime () {
    return actionDateTime;

  }  //- end getGalagoActionDateTime

}  //- end class VersionInfo
