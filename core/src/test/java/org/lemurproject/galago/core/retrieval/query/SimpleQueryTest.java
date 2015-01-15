// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class SimpleQueryTest {

    @Test
    public void testSingleTerm() {
        Node result = SimpleQuery.parseTree("a");
        assertEquals("#combine( #text:a() )", result.toString());
    }

    @Test
    public void testManyTerms() {
        Node result = SimpleQuery.parseTree("a b c");
        assertEquals("#combine( #text:a() #text:b() #text:c() )", result.toString());
    }

    @Test
    public void testScale() {
        Node result = SimpleQuery.parseTree("a^2");
        assertEquals("#combine( #scale:2.0( #text:a() ) )", result.toString());
    }

    @Test
    public void testField() {
        Node result = SimpleQuery.parseTree("f:a");
        assertEquals("#combine( #inside( #text:a() #field:f() ) )", result.toString());
    }

    @Test
    public void testPhrase() {
        Node result = SimpleQuery.parseTree("\"a b\"");
        assertEquals("#combine( #ordered:1( #text:a() #text:b() ) )", result.toString());
    }

    @Test
    public void testComplex() {
        Node result = SimpleQuery.parseTree("fi:\"a c\"^7");
        assertEquals(
                "#combine( #scale:7.0( #inside( #ordered:1( #text:a() #text:c() ) #field:fi() ) ) )",
                result.toString());
    }
}
