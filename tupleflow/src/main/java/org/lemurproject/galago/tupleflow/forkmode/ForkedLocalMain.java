// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.forkmode;

import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.GalagoConf;
import org.lemurproject.galago.tupleflow.execution.JobExecutor;
import org.lemurproject.galago.tupleflow.execution.LocalStageExecutor;
import org.lemurproject.galago.tupleflow.execution.StageExecutionStatus;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Based on LocalStageExecutor
 * @author jfoley
 * @see LocalStageExecutor
 */
public class ForkedLocalMain {

  public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException {
    Logger logger = Logger.getLogger(JobExecutor.class
            .toString());

    String stageDescriptionFile = args[0];

    logger.info(
            "Initializing: " + stageDescriptionFile);

    // This should also trigger the static initializer on GalagoConf which will load the parameters if need be.
    // It also nicely dumps them to a log so you should be able to tell if its using the file you think you're using.
    logger.info(".galago.conf: "+GalagoConf.getAllOptions().toString());

    FileUtility.waitUntilFileExists(stageDescriptionFile);

    StageExecutionStatus context = new LocalStageExecutor().execute(stageDescriptionFile);

    if (context.getExceptions()
            .size() > 0) {
      for (Exception exception : context.getExceptions()) {
        logger.severe("Exception thrown: " + exception.toString());
      }
      System.exit(1); // force quit on any remaining threads (particularly: NetworkCounter)
    } else {
      logger.info("Local Stage complete");
    }
  }
}
