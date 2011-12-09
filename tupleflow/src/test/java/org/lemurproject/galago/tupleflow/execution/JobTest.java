
package org.lemurproject.galago.tupleflow.execution;

import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.types.XMLFragment;

/**
 *
 * @author trevor
 */
public class JobTest extends TestCase {
    public JobTest(String testName) {
        super(testName);
    }

    public void testToDotString() {
        Job job = new Job();

        Stage a = new Stage("a");
        a.add(new StageConnectionPoint(ConnectionPointType.Output,
                "c", new XMLFragment.NodePathOrder()));
        job.add(a);
        Stage b = new Stage("b");
        b.add(new StageConnectionPoint(ConnectionPointType.Input,
                "c", new XMLFragment.NodePathOrder()));
        job.add(b);
        job.connect("a", "b", ConnectionAssignmentType.Combined);

        String expected =
        "digraph {\n" +
        "  a -> b [label=\"a-c\"];\n" +
        "  a;\n" +
        "  b;\n" +
        "}\n";

        assertEquals(expected, job.toDotString());
    }

}
