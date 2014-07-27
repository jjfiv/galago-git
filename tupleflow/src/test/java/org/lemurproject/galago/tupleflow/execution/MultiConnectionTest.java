/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.tupleflow.execution;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.types.TupleflowString;
import org.lemurproject.galago.tupleflow.types.XMLFragment;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *  Tests the connection of two stages with multiple connections between them.
 * 
 * @author sjh
 */
public class MultiConnectionTest {
  @Test
  public void testMultiConnections() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-a", new TupleflowString.ValueOrder()));
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-b", new XMLFragment.NodePathOrder()));
    one.add(new StepInformation(Generator.class));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-a", new TupleflowString.ValueOrder()));
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-b", new XMLFragment.NodePathOrder()));
    two.add(new StepInformation(Receiver.class));
    job.add(two);

    // this connect function should link BOTH out/in pairs
    job.connect("one", "two", ConnectionAssignmentType.Combined);

    ErrorStore store = new ErrorStore();
    Verification.verify(job, store);
    
    JobExecutor.runLocally(job, store, Parameters.instance());
    if (store.hasStatements()) {
      throw new RuntimeException(store.toString());
    }
  }

  public static class Generator implements ExNihiloSource {

    TupleFlowParameters params;

    public Generator(TupleFlowParameters params) {
      this.params = params;
    }

    @Override
    public void run() throws IOException {
      // now try to write a couple of data items
      Processor conna = params.getTypeWriter("conn-a");
      conna.process(new TupleflowString("TEST-CONN-A-1"));
      conna.process(new TupleflowString("TEST-CONN-A-2"));
      conna.close();

      Processor connb = params.getTypeWriter("conn-b");
      connb.process(new XMLFragment("TEST-CONN-B-1", "PAYLOAD-1"));
      connb.process(new XMLFragment("TEST-CONN-B-2", "PAYLOAD-2"));
      connb.close();
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
      if (!parameters.writerExists("conn-a", TupleflowString.class.getName(), TupleflowString.ValueOrder.getSpec())) {
        throw new IOException("Could not find the conn-a connection writer.");
      }
      if (!parameters.writerExists("conn-b", XMLFragment.class.getName(), XMLFragment.NodePathOrder.getSpec())) {
        throw new IOException("Could not find the conn-b connection writer.");
      }
    }
  }

  public static class Receiver implements ExNihiloSource {

    TupleFlowParameters params;

    public Receiver(TupleFlowParameters params) {
      this.params = params;
    }

    @Override
    public void run() throws IOException {
      // now try to read a couple of data items
      TypeReader conna = params.getTypeReader("conn-a");
      assertEquals(conna.read().toString(), new TupleflowString("TEST-CONN-A-1").toString());
      assertEquals(conna.read().toString(), new TupleflowString("TEST-CONN-A-2").toString());

      TypeReader connb = params.getTypeReader("conn-b");
      assertEquals(connb.read().toString(), new XMLFragment("TEST-CONN-B-1", "PAYLOAD-1").toString());
      assertEquals(connb.read().toString(), new XMLFragment("TEST-CONN-B-2", "PAYLOAD-2").toString());
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
      if (!parameters.readerExists("conn-a", TupleflowString.class.getName(), TupleflowString.ValueOrder.getSpec())) {
        throw new IOException("Could not find the conn-a connection reader.");
      }

      if (!parameters.readerExists("conn-b", XMLFragment.class.getName(), XMLFragment.NodePathOrder.getSpec())) {
        throw new IOException("Could not find the conn-a connection reader.");
      }
    }
  }
}
