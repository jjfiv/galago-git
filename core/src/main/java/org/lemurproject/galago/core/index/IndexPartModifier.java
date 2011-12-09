// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.io.PrintStream;
import org.lemurproject.galago.core.index.Index.IndexComponentReader;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 * Any class that implements this interface actually modifies a StructuredIndexPartReader,
 * meaning the data stored in the modifier is not standalone. Implementations of this class
 * should provide interpretations of the raw iterator data to provide modification data.
 *
 * Note that modifiers are usually built post-hoc from the index (e.g. topdocs for posting lists),
 * therefore the source part that built the modifier is stored as part of the structure.
 *
 * @author irmarc
 */
public interface IndexPartModifier extends IndexComponentReader {

  /**
   * Closes the modifier file.
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Returns the string form of the part this modifier was built from.
   * @return
   */
  public String getSourcePart();

  /**
   * Returns the string form of the modifier. This is described/expected from a node in its parameters,
   * as "mod=name", for example, "mod=topdocs".
   * @return
   */
  public String getModifierName();

  /**
   * Given a node, will produce the modification for that node
   * if it is eligible. Otherwise produces null.
   * @param node
   * @return
   */
  public Object getModification(Node node) throws IOException;

  /**
   * Returns true is the provided node object is eligible for modification.
   * @param node
   * @return
   */
  public boolean isEligible(Node node);

  /**
   * Dumps the contents of the modifier file. Basically just for visual inspection.
   */
  public void printContents(PrintStream out) throws IOException;
}
