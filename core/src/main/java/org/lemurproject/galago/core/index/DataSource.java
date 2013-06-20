// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;

/**
 * This is the shared interface for moving data sources.
 * Note that this 
 * @author jfoley
 */
public interface DataSource {
  public void reset() throws IOException;
  public boolean isDone();
  
  public int currentCandidate();
  public boolean hasMatch(int id);
  
  public void movePast(int id);
  public void moveTo(int id);
}
