/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.RankedDocumentModel;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class AnnotatedNodeTest {

  @Test
  public void testAnnotatedNodes() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    files[0].delete();
    Utility.deleteDirectory(files[1]);
    File indexFile = files[2];
    try {
      LocalRetrieval r = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), Parameters.instance());

      String qtext = "#combine( sample document )";
      Node qnode = StructuredQuery.parse(qtext);
      qnode = r.transformQuery(qnode, Parameters.instance());
      ProcessingModel proc = new RankedDocumentModel(r);
      Parameters p = Parameters.instance();
      p.set("requested", 100);
      p.set("annotate", true);
      ScoredDocument[] results = proc.execute(qnode, p);
      AnnotatedNode prev = null;
      for (ScoredDocument d : results) {
        assertNotNull(d.annotation);
        AnnotatedNode anode = d.annotation;
        assertTrue(anode.atCandidate);
        if (prev != null) {
          assert (Double.parseDouble(prev.returnValue) > Double.parseDouble(anode.returnValue));
        }
        prev = anode;
      }
    } finally {
      if (indexFile != null) {
        Utility.deleteDirectory(indexFile);
      }
    }
  }
}
