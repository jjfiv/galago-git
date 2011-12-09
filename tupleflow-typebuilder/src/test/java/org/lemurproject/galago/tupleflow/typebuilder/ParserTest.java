package org.lemurproject.galago.tupleflow.typebuilder;

import org.lemurproject.galago.tupleflow.typebuilder.TypeSpecification;
import org.lemurproject.galago.tupleflow.typebuilder.Direction;
import org.antlr.runtime.RecognitionException;
import junit.framework.TestCase;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.lemurproject.galago.tupleflow.typebuilder.FieldSpecification.DataType;

/**
 *
 * @author trevor
 */
public class ParserTest extends TestCase {
    public void testParser() throws RecognitionException {
        String template =
                "package org.lemurproject.galago.core.types; \n" +
                "type DocumentExtent {\n" +
                "  bytes extentName;\n" +
                "  long document;\n" +      
                "  int begin;\n" +
                "  int end;\n" +
                "  order: +extentName +document ;\n" +
                "  order: ;\n" +
                "}";
        
        ANTLRStringStream input = new ANTLRStringStream(template);
        GalagoTypeBuilderLexer lexer = new GalagoTypeBuilderLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        GalagoTypeBuilderParser parser = new GalagoTypeBuilderParser(tokens);
        TypeSpecification spec = parser.type_def();
        
        assertEquals("DocumentExtent", spec.getTypeName());
        assertEquals("org.lemurproject.galago.core.types", spec.getPackageName());
        
        assertEquals("extentName", spec.getFields().get(0).name);
        assertEquals(DataType.BYTES, spec.getFields().get(0).type);
        
        assertEquals("document", spec.getOrders().get(0).getOrderedFields().get(1).name);
        assertEquals(Direction.ASCENDING,
                spec.getOrders().get(0).getOrderedFields().get(1).direction);
    }
}
