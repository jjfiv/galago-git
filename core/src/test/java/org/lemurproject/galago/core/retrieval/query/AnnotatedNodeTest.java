/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.RankedDocumentModel;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class AnnotatedNodeTest extends TestCase {

  public AnnotatedNodeTest(String name) {
    super(name);
  }

  public void testAnnotatedNodes() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    files[0].delete();
    Utility.deleteDirectory(files[1]);
    File indexFile = files[2];
    try {
      LocalRetrieval r = (LocalRetrieval) RetrievalFactory.instance(indexFile.getAbsolutePath(), new Parameters());

      String qtext = "#combine( sample document )";
      Node qnode = StructuredQuery.parse(qtext);
      qnode = r.transformQuery(qnode, new Parameters());
      ProcessingModel proc = new RankedDocumentModel(r);
      Parameters p = new Parameters();
      p.set("requested", 100);
      p.set("annotate", true);
      ScoredDocument[] results = proc.execute(qnode, p);
      AnnotatedNode prev = null;
      for (ScoredDocument d : results) {
        assert (d.annotation != null);
        AnnotatedNode anode = d.annotation;
        assert (anode.atCandidate == true);
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
