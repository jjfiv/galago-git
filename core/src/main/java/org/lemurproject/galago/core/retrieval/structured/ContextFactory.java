/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.structured;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Sole purpose of this class is to generate contexts for retrieval.
 * Right now the contexts available seem to serve orthogonal purposes. It would be great
 * to have mixins or traits for the contexts, but that's not an option. Eventually the
 * contexts will have to be refactored.
 *
 * @author irmarc
 */
public class ContextFactory {

  // Can't instantiate it
  private ContextFactory() {
  }

  public static ScoringContext createContext(Parameters p) {
    if (p.get("mod", "none").equals("topdocs")) {
      return new TopDocsContext();
    } else if (p.containsKey("passageSize") || p.containsKey("passageShift")) {
      return new PassageScoringContext();
    } else {
      return new ScoringContext();
    }
  }
}
