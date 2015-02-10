// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class SimpleQueryTest {

    @Test
    public void testSingleTerm() throws IOException {
        Node result = SimpleQuery.parseTree("a");
        assertEquals("#combine( #text:a() )", result.toString());
    }

    @Test
    public void testManyTerms() throws IOException {
        Node result = SimpleQuery.parseTree("a b c");
        assertEquals("#combine( #text:a() #text:b() #text:c() )", result.toString());
    }

    @Test
    public void testScale() throws IOException {
        Node result = SimpleQuery.parseTree("a^2");
        assertEquals("#combine( #scale:2.0( #text:a() ) )", result.toString());
    }

    @Test
    public void testField() throws IOException {
        Node result = SimpleQuery.parseTree("f:a");
        assertEquals("#combine( #inside( #text:a() #field:f() ) )", result.toString());
    }

    @Test
    public void testPhrase() throws IOException {
        Node result = SimpleQuery.parseTree("\"a b\"");
        assertEquals("#combine( #ordered:1( #text:a() #text:b() ) )", result.toString());
    }

    @Test
    public void testComplex() throws IOException {
        Node result = SimpleQuery.parseTree("fi:\"a c\"");
        assertEquals(
                "#combine( #inside( #ordered:1( #text:a() #text:c() ) #field:fi() ) )",
                result.toString());
    }

    @Test
    public void testComplex2() throws IOException {
        Node result = SimpleQuery.parseTree("fi:\"a c\"^7");
        assertEquals(
                "#combine( #scale:7.0( #inside( #ordered:1( #text:a() #text:c() ) #field:fi() ) ) )",
                result.toString());
    }

    @Test
    public void testFieldWithEmail() throws IOException {
        Node result = SimpleQuery.parseTree("email:\"michaelz@cs.umass.edu\"");
        String expectedResult = "#combine( #inside( #ordered:1( #text:michaelz() #text:cs() #text:umass() #text:edu() ) #field:email() ) )";
        assertEquals(expectedResult, result.toString());

        result = SimpleQuery.parseTree("email:michaelz@cs.umass.edu");
        assertEquals(expectedResult, result.toString());

    }

    @Test
    public void testFieldWithPunctuation() throws IOException {
        Node result = SimpleQuery.parseTree("author:\"W. Bruce Croft\"");
        String expectedResult = "#combine( #inside( #ordered:1( #text:w() #text:bruce() #text:croft() ) #field:author() ) )";
        assertEquals(expectedResult, result.toString());
    }
}
