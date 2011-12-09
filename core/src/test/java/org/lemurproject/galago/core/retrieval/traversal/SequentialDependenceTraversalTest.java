// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.traversal.SequentialDependenceTraversal;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class SequentialDependenceTraversalTest extends TestCase {
    File indexPath;

    public SequentialDependenceTraversalTest(String testName) {
        super(testName);
    }

    @Override
    public void setUp() throws FileNotFoundException, IOException {
        indexPath = LocalRetrievalTest.makeIndex();
    }

    @Override
    public void tearDown() throws IOException{
        Utility.deleteDirectory(indexPath);
    }

    public void testTraversal() throws Exception {
        DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
        LocalRetrieval retrieval = new LocalRetrieval(index, new Parameters());
        SequentialDependenceTraversal traversal = new SequentialDependenceTraversal(retrieval);
        Node tree = StructuredQuery.parse("#seqdep( cat dog rat )");
        StringBuilder transformed = new StringBuilder();
        transformed.append("#combine:0=0.8:1=0.15:2=0.05( ");
        transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
        transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
        transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
        Node result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

        // now change weights
        Parameters p = new Parameters();
        p.set("uniw", 0.75);
        p.set("odw", 0.10);
        p.set("uww", 0.15);
        retrieval = new LocalRetrieval(index, p);
        traversal = new SequentialDependenceTraversal(retrieval);
        tree = StructuredQuery.parse("#seqdep( cat dog rat )");
        transformed = new StringBuilder();
        transformed.append("#combine:0=0.75:1=0.1:2=0.15( ");
        transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
        transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
        transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
        result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

        // now change weights via the operator
        tree = StructuredQuery.parse("#seqdep:uniw=0.55:odw=0.27:uww=0.18( cat dog rat )");
        transformed = new StringBuilder();
        transformed.append("#combine:0=0.55:1=0.27:2=0.18( ");
        transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
        transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
        transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
        result = StructuredQuery.copy(traversal, tree);

        assertEquals(transformed.toString(), result.toString());

    }
}
