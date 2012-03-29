/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.tupleflow.execution;

import java.io.IOException;
import java.util.List;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

/**
 * Tests the connection of stages (single/distributed) using (combined/each) connections
 * 
 *  [SIMPLE CONNECTIONS single data streams]
 *  1.1: single-single-combined
 *   1 --> 2 [Combined]
 *
 *  1.2: single-multi-single (each, combined)
 *   1 --> 2 [Each]  no data
 *   2 --> 3 [Combined]
 *
 *  1.3: single-multi-multi-single (each, each, combined)
 *   1 --> 2 [Each]  no data
 *   2 --> 3 [Each]
 *   3 --> 4 [Combined]
 *
 * 
 *  [COMPLEX MULTICONNECTIONS multiple data streams]
 *  2.1: two connection types incomming (single-multi-combined + single-multi-each)
 *   1 --> 2 [Combined]  no data
 *   1 --> 3 [Combined]  no data
 *   2 --> 4 [Combined]
 *   3 --> 4 [Each]
 *
 *  2.2: two connection types incomming (multi-multi-combined + multi-multi-each)
 *   1 --> 2 [Combined]
 *   1 --> 3 [Each]
 *   2 --> 4 [Combined]
 *   3 --> 4 [Each]
 *
 *  2.3: two connection types incomming (multi-multi-combined + multi-multi-each)
 *   1 --> 2 [Each]
 *   1 --> 3 [Each]
 *   2 --> 4 [Combined]
 *   3 --> 4 [Each]
 * 
 * @author sjh
 */
public class ConnectionTest extends TestCase {

  public ConnectionTest(String name) {
    super(name);
  }

  public void testSingleSingleComb() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-2", new TupleflowString.ValueOrder()));
    one.add(new Step(Generator.class, Parameters.parse("{\"name\":\"one\", \"conn\":[\"conn-1-2\"]}")));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    two.add(new InputStep("conn-1-2"));
    two.add(new Step(Receiver.class, Parameters.parse("{\"expectedCount\":10}")));
    job.add(two);

    job.connect("one", "two", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);
    JobExecutor.runLocally(job, err, new Parameters());
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testSingleMultiEach() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-2", new TupleflowString.ValueOrder()));
    one.add(new Step(NullSource.class));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    two.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-2-3", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parse("{\"name\":\"two\", \"conn\":[\"conn-2-3\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-3", new TupleflowString.ValueOrder()));
    three.add(new InputStep("conn-2-3"));
    three.add(new Step(Receiver.class, Parameters.parse("{\"expectedCount\":20}")));
    job.add(three);

    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("two", "three", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);
    JobExecutor.runLocally(job, err, new Parameters());
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

    /*
  public void testMultiMultiEach() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-2", new TupleflowString.ValueOrder()));
    one.add(new Step(NullSource.class));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    two.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-2-3", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parse("{\"name\":\"two\", \"conn\":[\"conn-2-3\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-3", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-4", new TupleflowString.ValueOrder()));
    three.add(new InputStep("conn-2-3"));
    three.add(new Step(PassThrough.class));
    three.add(new OutputStep("conn-3-4"));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-4", new TupleflowString.ValueOrder()));
    four.add(new InputStep("conn-3-4"));
    four.add(new Step(Receiver.class, Parameters.parse("{\"expectedCount\":20}")));
    job.add(four);


    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("two", "three", ConnectionAssignmentType.Each);
    job.connect("three", "four", ConnectionAssignmentType.Combined);

    System.err.println(job.toDotString());

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, new Parameters());
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }
    */
  

  
  public static class NullSource implements ExNihiloSource {

    @Override
    public void run() throws IOException {
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) throws IOException {
    }
  }

  public static class Generator implements ExNihiloSource {

    TupleFlowParameters params;

    public Generator(TupleFlowParameters params) {
      this.params = params;
    }

    @Override
    public void run() throws IOException {
      String name = params.getJSON().getString("name");
      String fmt = "(" + name + "-%d)-i%d-(%s)";
      for (String conn : (List<String>) params.getJSON().getList("conn")) {
        try {
          Processor<TupleflowString> c = params.getTypeWriter(conn);
          Sorter s = new Sorter(new TupleflowString.ValueOrder());
          s.setProcessor(c);
          for (int i = 0; i < 10; i++) {
            s.process(new TupleflowString(String.format(fmt, params.getInstanceId(), i, conn)));
          }
          s.close();
        } catch (IncompatibleProcessorException ex) {
          throw new RuntimeException("FAILED TO GENERATE DATA FOR CONNECTION:" + conn);
        }
      }
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) throws IOException {
      if (!parameters.getJSON().isString("name")) {
        handler.addError("Could not find the name of the stage in parameters");
      }
      if (!parameters.getJSON().isList("conn", Type.STRING)) {
        handler.addError("Could not find any connections specified in parameters");
      }
      for (String conn : (List<String>) parameters.getJSON().getList("conn")) {
        if (!parameters.writerExists(conn, TupleflowString.class.getName(), TupleflowString.ValueOrder.getSpec())) {
          handler.addError("Could not verify connection: " + conn);
        }
      }
    }
  }

  public static class PassThrough extends StandardStep<TupleflowString, TupleflowString> {

    TupleFlowParameters params;
    String suffix;

    public PassThrough(TupleFlowParameters params) {
      this.params = params;
      String conn = params.getJSON().getString("name");
      suffix = "-("+conn+")";
    }

    @Override
    public void process(TupleflowString s) throws IOException {
      processor.process(new TupleflowString(s + suffix));
    }

  public static String getInputClass(TupleFlowParameters parameters) {
    return "org.lemurproject.galago.tupleflow.types.TupleflowString";
  }

  public static String getOutputClass(TupleFlowParameters parameters) {
    return "org.lemurproject.galago.tupleflow.types.TupleflowString";
  }

  public static String[] getOutputOrder(TupleFlowParameters parameters) {
    return TupleflowString.ValueOrder.getSpec();
  }


    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) throws IOException {
      if (!parameters.getJSON().isString("name")) {
        handler.addError("Could not find the name of the stage in parameters");
      }
    }
  }

  public static class Receiver implements Processor<TupleflowString> {

    TupleFlowParameters params;
    int counter = 0;

    public Receiver(TupleFlowParameters params) {
      this.params = params;
    }

    @Override
    public void process(TupleflowString s) throws IOException {
      System.err.println("REC - " + params.getInstanceId() + " : " + s.value);
      counter += 1;
    }

    @Override
    public void close() throws IOException {
      assert counter == params.getJSON().getLong("expectedCount");
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) throws IOException {
      // nothing to verify yet
    }

    public static String getInputClass(TupleFlowParameters parameters) {
      return TupleflowString.class.getName();
    }
  }
}
