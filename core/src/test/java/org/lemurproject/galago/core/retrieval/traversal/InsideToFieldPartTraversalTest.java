/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class InsideToFieldPartTraversalTest {

    private File indexPath;

    @Before
    public void setUp() throws FileNotFoundException, IOException, IncompatibleProcessorException {
        indexPath = LocalRetrievalTest.makeIndex();
    }

    @After
    public void tearDown() throws IOException {
        FSUtil.deleteDirectory(indexPath);
    }

    @Test
    public void testTraversal() throws Exception {
        DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
        LocalRetrieval retrieval = new LocalRetrieval(index);
        TextFieldRewriteTraversal rewriter = new TextFieldRewriteTraversal(retrieval);
        Parameters inner1 = Parameters.create();
        inner1.set("extents", DiskExtentIterator.class.getName());
        inner1.set("counts", DiskCountIterator.class.getName());

        InsideToFieldPartTraversal traversal = new InsideToFieldPartTraversal(retrieval);
        Parameters inner2 = Parameters.create();
        inner2.set("extents", DiskExtentIterator.class.getName());
        inner2.set("counts", DiskCountIterator.class.getName());
        traversal.availableParts.set("field.subject", inner2);

        Node q1 = StructuredQuery.parse("#combine( cat dog.title donkey.subject absolute.subject)");
        Node q2 = rewriter.traverse(q1, Parameters.create()); // converts #text to #extents...
        Node q3 = traversal.traverse(q2, Parameters.create()); // converts #inside to #extents...

        StringBuilder transformed = new StringBuilder();

        transformed.append("#combine( ");
        transformed.append("#extents:cat:part=postings() ");
        transformed.append("#inside( #extents:dog:part=postings() ");
        transformed.append("#extents:title:part=extents() ) ");
        transformed.append("#extents:donkey:part=field.subject() ");
        transformed.append("#extents:absolute:part=field.subject() )");

        assertEquals(transformed.toString(), q3.toString());

        Node q4 = StructuredQuery.parse("#inside( #ordered:1( #text:james() #text:allan() ) #field:author() )");
        Node q5 = rewriter.traverse(q4, Parameters.create()); // converts #text to #extents...
        assertEquals("#inside( #ordered:1( #extents:james:part=postings() #extents:allan:part=postings() ) #extents:author:part=extents() )", q5.toString());
        Node q6 = traversal.traverse(q5, Parameters.create()); // converts #inside to #extents...

        // shouldn't change with the last traversal
        assertEquals(q5.toString(), q6.toString());

    }
}
