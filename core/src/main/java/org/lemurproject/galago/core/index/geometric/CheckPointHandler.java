/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * 
 * @author sjh
 */
public class CheckPointHandler {
  private String path;
  
  public void setDirectory(String dir){
    this.path = dir + File.separator + "checkpoint" ;
    Utility.makeParentDirectories( path );
  }
  
  public void saveCheckpoint(Parameters checkpoint) throws IOException {
    Utility.copyStringToFile( checkpoint.toString() , new File(path) );
  }
  
  public Parameters getRestore() throws IOException {
    return Parameters.parse( new File(path) );
  }
}
