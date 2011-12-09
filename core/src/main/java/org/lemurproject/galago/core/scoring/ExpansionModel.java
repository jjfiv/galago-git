// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 *
 * Generic interface for an expander -- supports any kind of query expansion (as opposed to *just* term expansion).
 *
 * @author irmarc
 */
public interface ExpansionModel {

    public void initialize() throws Exception;
    public void cleanup() throws Exception;

    public List<WeightedTerm> generateGrams(List<ScoredDocument> initialResults)
            throws IOException;

    public Node generateExpansionQuery(List<ScoredDocument> initialResults, int fbTerms, 
            Set<String> exclusionTerms) throws IOException;
}
