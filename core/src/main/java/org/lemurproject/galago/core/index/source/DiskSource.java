// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import java.io.IOException;

/**
 * This is the shared interface for moving data sources.
 * @author jfoley
 */
public interface DiskSource {
  /**
   * reset the iterator to the beginning.
   * Often doubles as an initialization step.
   * @see BTreeValueSource for an implementation
   * @throws IOException 
   */
  public void reset() throws IOException;
  public boolean isDone();
  
    
  /**
   * This method determines whether a Source contains a value for all identifiers or not.
   * Lengths has all candidates, whereas counts of a particular term does not.
   * @return true if this iterator contains all values.
   */
  public boolean hasAllCandidates();
  
  /**
   * This method returns the total number of entries available.
   * @return 
   */
  public long totalEntries();
    
  public String key();
  public int currentCandidate();
  public boolean hasMatch(int id);
  
  public void movePast(int id) throws IOException;
  public void syncTo(int id) throws IOException;
}
