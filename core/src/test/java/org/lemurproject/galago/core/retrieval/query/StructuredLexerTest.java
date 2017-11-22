// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.query;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.StructuredLexer.Token;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 * @author trevor
 */
public class StructuredLexerTest {

  @Test
  public void testTokens() throws Exception {
    StructuredLexer lexer = new StructuredLexer();
    List<Token> tokens = lexer.tokens("#op:this=that:a( b c d ).e");
    Iterator<Token> iterator = tokens.iterator();

    Token t = iterator.next();
    assertEquals("#", t.text);
    assertEquals(0, t.position);

    t = iterator.next();
    assertEquals("op", t.text);
    assertEquals(1, t.position);

    t = iterator.next();
    assertEquals(":", t.text);
    assertEquals(3, t.position);

    t = iterator.next();
    assertEquals("this", t.text);
    assertEquals(4, t.position);

    t = iterator.next();
    assertEquals("=", t.text);
    assertEquals(8, t.position);

    t = iterator.next();
    assertEquals("that", t.text);
    assertEquals(9, t.position);

    t = iterator.next();
    assertEquals(":", t.text);
    assertEquals(13, t.position);

    t = iterator.next();
    assertEquals("a", t.text);
    assertEquals(14, t.position);

    t = iterator.next();
    assertEquals("(", t.text);
    assertEquals(15, t.position);

    t = iterator.next();
    assertEquals("b", t.text);
    assertEquals(17, t.position);

    t = iterator.next();
    assertEquals("c", t.text);
    assertEquals(19, t.position);

    t = iterator.next();
    assertEquals("d", t.text);
    assertEquals(21, t.position);

    t = iterator.next();
    assertEquals(")", t.text);
    assertEquals(23, t.position);

    t = iterator.next();
    assertEquals(".", t.text);
    assertEquals(24, t.position);

    t = iterator.next();
    assertEquals("e", t.text);
    assertEquals(25, t.position);

    assertFalse(iterator.hasNext());
  }

  @Test
  public void testEscapes() throws IOException {
    StructuredLexer lexer = new StructuredLexer();
    List<Token> tokens = lexer.tokens("@/b c d/");
    Iterator<Token> iterator = tokens.iterator();

    Token t = iterator.next();
    assertEquals("b c d", t.text);
    assertEquals(0, t.position);
  }

  @Test
  public void testSimpleDotTokens () throws Exception {
    //- A dot at the end of a query, or with no field specifier after
    //  it should be tokenized without the dot.
    StructuredLexer lexer = new StructuredLexer ();
    List<Token> tokens = lexer.tokens ("world. ");
    Iterator<Token> iterator = tokens.iterator();

    Token t = iterator.next();
    assertEquals("world", t.text);
    assertEquals(0, t.position);

    assertFalse(iterator.hasNext());
  }

  @Test
  public void testStructuredDotTokens () throws Exception {
    //- A structured query term with a field specification should tokenzie
    //  the dot.  Note that if a dot appears at the end of such a query,
    //  the parse should not fail, but it will still be an illegal query.
    StructuredLexer lexer = new StructuredLexer ();
    List<Token> tokens = lexer.tokens ("#combine(world.headline)");
    Iterator<Token> iterator = tokens.iterator();

    Token t = iterator.next();
    assertEquals("#", t.text);
    assertEquals(0, t.position);

    t = iterator.next();
    assertEquals("combine", t.text);
    assertEquals(1, t.position);

    t = iterator.next();
    assertEquals("(", t.text);
    assertEquals(8, t.position);

    t = iterator.next();
    assertEquals("world", t.text);
    assertEquals(9, t.position);

    t = iterator.next();
    assertEquals(".", t.text);
    assertEquals(14, t.position);

    t = iterator.next();
    assertEquals("headline", t.text);
    assertEquals(15, t.position);

    t = iterator.next();
    assertEquals(")", t.text);
    assertEquals(23, t.position);


    assertFalse(iterator.hasNext());
  }

  @Test
  public void testCombinedSimpleStructuredDotTokens () throws Exception {
    //- A mix of simple and structure query terms should either ignore the
    //  dot, or correctly tokenize it as a separator for a field term.
    StructuredLexer lexer = new StructuredLexer ();
    List<Token> tokens = lexer.tokens ("#combine(world.  world.headline)");
    Iterator<Token> iterator = tokens.iterator();

    Token t = iterator.next();
    assertEquals("#", t.text);
    assertEquals(0, t.position);

    t = iterator.next();
    assertEquals("combine", t.text);
    assertEquals(1, t.position);

    t = iterator.next();
    assertEquals("(", t.text);
    assertEquals(8, t.position);

    t = iterator.next();
    assertEquals("world", t.text);
    assertEquals(9, t.position);

    t = iterator.next();
    assertEquals("world", t.text);
    assertEquals(17, t.position);

    t = iterator.next();
    assertEquals(".", t.text);
    assertEquals(22, t.position);

    t = iterator.next();
    assertEquals("headline", t.text);
    assertEquals(23, t.position);

    t = iterator.next();
    assertEquals(")", t.text);
    assertEquals(31, t.position);

    assertFalse(iterator.hasNext());
  }

}
