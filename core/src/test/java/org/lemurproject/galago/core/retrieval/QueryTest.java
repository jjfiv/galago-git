// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.query.SimpleQuery.QueryTerm;
import org.lemurproject.galago.core.retrieval.query.SimpleQuery;
import junit.framework.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author trevor
 */
public class QueryTest extends TestCase {
    public QueryTest(String testName) {
        super(testName);
    }

    public void testSimpleQuery() {
        String query = "a b c d";
        List<QueryTerm> expected = new ArrayList<QueryTerm>();

        expected.add(new QueryTerm("a"));
        expected.add(new QueryTerm("b"));
        expected.add(new QueryTerm("c"));
        expected.add(new QueryTerm("d"));

        List<QueryTerm> actual = SimpleQuery.parse(query);
        assertEquals(expected, actual);
    }

    public void testComplicatedQuery() {
        String query = "f:aa^3.4 g:\"b c\"^9 \"l m\" j k d^8";
        List<QueryTerm> expected = new ArrayList<QueryTerm>();

        expected.add(new QueryTerm("aa", "f", 3.4));
        expected.add(new QueryTerm("b c", "g", 9));
        expected.add(new QueryTerm("l m"));
        expected.add(new QueryTerm("j"));
        expected.add(new QueryTerm("k"));
        expected.add(new QueryTerm("d", null, 8));

        List<QueryTerm> actual = SimpleQuery.parse(query);
        assertEquals(expected, actual);
    }
}
