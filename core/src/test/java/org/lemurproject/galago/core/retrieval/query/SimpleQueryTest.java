// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.query;

import org.lemurproject.galago.core.retrieval.query.SimpleQuery;
import org.lemurproject.galago.core.retrieval.query.Node;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class SimpleQueryTest extends TestCase {
    
    public SimpleQueryTest(String testName) {
        super(testName);
    }

    public void testSingleTerm() {
        Node result = SimpleQuery.parseTree("a");
        assertEquals("#text:a()", result.toString());
    }
    
    public void testManyTerms() {
        Node result = SimpleQuery.parseTree("a b c");
        assertEquals("#combine( #text:a() #text:b() #text:c() )", result.toString());
    }
    
    public void testScale() {
        Node result = SimpleQuery.parseTree("a^2");
        assertEquals("#scale:2.0( #text:a() )", result.toString());
    }
    
    public void testField() {
        Node result = SimpleQuery.parseTree("f:a");
        assertEquals("#inside( #text:a() #field:f() )", result.toString());
    }
    
    public void testPhrase() {
        Node result = SimpleQuery.parseTree("\"a b\"");
        assertEquals("#ordered:1( #text:a() #text:b() )", result.toString());
    }
    
    public void testComplex() {
        Node result = SimpleQuery.parseTree("fi:\"a c\"^7");
        assertEquals(
                "#scale:7.0( #inside( #ordered:1( #text:a() #text:c() ) #field:fi() ) )",
                result.toString());
    }
}
