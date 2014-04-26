// BSD License (http://lemurproject.org/galago-galago-license)

package org.lemurproject.galago.tupleflow.typebuilder;

import org.antlr.runtime.ANTLRFileStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public class ParserDriver {
  public static TypeSpecification getTypeSpecification(String fileName) throws IOException, RecognitionException {
    ANTLRFileStream input = new ANTLRFileStream(fileName);
    GalagoTypeBuilderLexer lexer = new GalagoTypeBuilderLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    GalagoTypeBuilderParser parser = new GalagoTypeBuilderParser(tokens);
    return parser.type_def();
  }
}
