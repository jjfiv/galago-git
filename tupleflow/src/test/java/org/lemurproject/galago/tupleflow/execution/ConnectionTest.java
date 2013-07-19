/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.tupleflow.execution;

import java.io.IOException;
import java.util.List;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

/**
 * Tests the connection of stages (single/distributed) using (combined/each)
 * connections
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
 *   4 --> 5 [Combined]
 *
 *  2.2: two connection types incomming (multi-multi-combined + multi-multi-each)
 *   1 --> 2 [Combined]  nodata
 *   1 --> 3 [Each]  nodata
 *   2 --> 4 [Combined]
 *   3 --> 4 [Each]
 *   4 --> 5 [Combined]
 *
 *  2.3: two connection types incomming (multi-multi-combined + multi-multi-each)
 *   1 --> 2 [Each]  nodata
 *   1 --> 3 [Each]  nodata
 *   2 --> 4 [Combined]
 *   3 --> 4 [Each]
 *   4 --> 5 [Combined]
 *
 *  2.4: two connections outgoing (multi-single-combined + multi-multi-each)
 *   1 --> 2 [Combined]  nodata
 *   2 --> 3 [Combined]
 *   2 --> 4 [Each]
 *   3 --> 5 [Combined]
 *   4 --> 5 [Combined]
 *
 *  2.5: two connections outgoing (multi-single-combined + multi-multi-each)
 *   1 --> 2 [Each]  nodata
 *   2 --> 3 [Combined]
 *   2 --> 4 [Each]
 *   3 --> 5 [Combined]
 *   4 --> 5 [Combined]
 *
 *  [GZIP CONNECTIONS single data streams]
 *  3.1: single-single-combined
 *   1 --> 2 [Combined] -- in GZIP
 *
 *  3.2: single-multi-single (each, combined)
 *   1 --> 2 [Each]  no data
 *   2 --> 3 [Combined]  -- in GZIP
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
    one.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"one\", \"conn\":[\"conn-1-2\"]}")));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    // should recieve 10 items from one
    two.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":10, \"connIn\" : [\"conn-1-2\"]}")));
    job.add(two);

    job.connect("one", "two", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);
    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
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
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-3\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-3", new TupleflowString.ValueOrder()));
    // should recieve 10 items from each instance of two (20 total)
    three.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":20, \"connIn\" : [\"conn-2-3\"]}")));
    job.add(three);

    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("two", "three", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);
    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

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
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-3\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-3", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-4", new TupleflowString.ValueOrder()));
    three.add(new Step(PassThrough.class, Parameters.parseString("{\"name\":\"three\", \"connIn\" : [\"conn-2-3\"], \"connOut\" : [\"conn-3-4\"]}")));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-4", new TupleflowString.ValueOrder()));
    // should recieve 10 items from each instance of two - they will be passed through three
    four.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":20, \"connIn\" : [\"conn-3-4\"]}")));
    job.add(four);


    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("two", "three", ConnectionAssignmentType.Each);
    job.connect("three", "four", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testSingleSingleIntoMulti() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-2", new TupleflowString.ValueOrder()));
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-3", new TupleflowString.ValueOrder()));
    one.add(new Step(NullSource.class));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    two.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-2-4", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-4\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-3", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-4", new TupleflowString.ValueOrder()));
    three.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"three\", \"conn\":[\"conn-3-4\"]}")));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-4", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-4", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-4-5", new TupleflowString.ValueOrder()));
    four.add(new Step(Merge.class, Parameters.parseString("{\"name\":\"four\", \"connIn\" : [\"conn-2-4\",\"conn-3-4\"], \"connOut\" : \"conn-4-5\"}")));
    job.add(four);


    Stage five = new Stage("five");
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-4-5", new TupleflowString.ValueOrder()));
    // two generates 10 items - all 10 should be passed to each instance of four (20)
    // three generates 10 items - they are distributed to instances of four
    // four passes all 30 to five
    five.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":30, \"connIn\" : [\"conn-4-5\"]}")));
    job.add(five);


    job.connect("one", "two", ConnectionAssignmentType.Combined);
    job.connect("one", "three", ConnectionAssignmentType.Combined);
    job.connect("two", "four", ConnectionAssignmentType.Combined);
    job.connect("three", "four", ConnectionAssignmentType.Each);
    job.connect("four", "five", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testSingleMultiIntoMulti() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-2", new TupleflowString.ValueOrder()));
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-3", new TupleflowString.ValueOrder()));
    one.add(new Step(NullSource.class));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    two.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-2-4", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-4\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-3", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-4", new TupleflowString.ValueOrder()));
    three.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"three\", \"conn\":[\"conn-3-4\"]}")));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-4", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-4", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-4-5", new TupleflowString.ValueOrder()));
    four.add(new Step(Merge.class, Parameters.parseString("{\"name\":\"four\", \"connIn\" : [\"conn-2-4\",\"conn-3-4\"], \"connOut\" : \"conn-4-5\"}")));
    job.add(four);


    Stage five = new Stage("five");
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-4-5", new TupleflowString.ValueOrder()));
    // two generates 10 items - all 10 should be passed to each instance of four (20)
    // three generates 10 items per instance (20) - they are distributed to instances of four
    // four passes all 40 to five
    five.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":40, \"connIn\" : [\"conn-4-5\"]}")));
    job.add(five);


    job.connect("one", "two", ConnectionAssignmentType.Combined);
    job.connect("one", "three", ConnectionAssignmentType.Each);
    job.connect("two", "four", ConnectionAssignmentType.Combined);
    job.connect("three", "four", ConnectionAssignmentType.Each);
    job.connect("four", "five", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testMultiMultiIntoMulti() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-2", new TupleflowString.ValueOrder()));
    one.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-1-3", new TupleflowString.ValueOrder()));
    one.add(new Step(NullSource.class));
    job.add(one);

    Stage two = new Stage("two");
    two.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-2", new TupleflowString.ValueOrder()));
    two.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-2-4", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-4\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-1-3", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-4", new TupleflowString.ValueOrder()));
    three.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"three\", \"conn\":[\"conn-3-4\"]}")));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-4", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-4", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-4-5", new TupleflowString.ValueOrder()));
    four.add(new Step(Merge.class, Parameters.parseString("{\"name\":\"four\", \"connIn\" : [\"conn-2-4\",\"conn-3-4\"], \"connOut\" : \"conn-4-5\"}")));
    job.add(four);


    Stage five = new Stage("five");
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-4-5", new TupleflowString.ValueOrder()));
    // two generates 10 items per instance (20) - all 20 are duplicated for each instance of four - creating 40 total
    // three generates 10 items per instance (20) - passed through four without duplication
    // should recieve 60 total
    five.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":60, \"connIn\" : [\"conn-4-5\"]}")));
    job.add(five);


    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("one", "three", ConnectionAssignmentType.Each);
    job.connect("two", "four", ConnectionAssignmentType.Combined);
    job.connect("three", "four", ConnectionAssignmentType.Each);
    job.connect("four", "five", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testSingleIntoSingleMulti() throws Exception {
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
            "conn-2-x", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-x\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-x", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-5", new TupleflowString.ValueOrder()));
    three.add(new Step(PassThrough.class, Parameters.parseString("{\"name\":\"three\", \"connIn\" : [\"conn-2-x\"], \"connOut\" : [\"conn-3-5\"]}")));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-x", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-4-5", new TupleflowString.ValueOrder()));
    four.add(new Step(PassThrough.class, Parameters.parseString("{\"name\":\"four\", \"connIn\" : [\"conn-2-x\"], \"connOut\" : [\"conn-4-5\"]}")));
    job.add(four);


    Stage five = new Stage("five");
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-5", new TupleflowString.ValueOrder()));
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-4-5", new TupleflowString.ValueOrder()));
    // two generates 10 items
    // the items are sent to three (combined - 10)
    // the items are sent to four (each - 10)
    // three and four send items to five
    // should recieve 20 total
    five.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":20, \"connIn\" : [\"conn-3-5\",\"conn-4-5\"]}")));
    job.add(five);

    job.connect("one", "two", ConnectionAssignmentType.Combined);
    job.connect("two", "three", ConnectionAssignmentType.Combined);
    job.connect("two", "four", ConnectionAssignmentType.Each);
    job.connect("three", "five", ConnectionAssignmentType.Combined);
    job.connect("four", "five", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testMultiIntoSingleMulti() throws Exception {
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
            "conn-2-x", new TupleflowString.ValueOrder()));
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-x\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-x", new TupleflowString.ValueOrder()));
    three.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-3-5", new TupleflowString.ValueOrder()));
    three.add(new Step(PassThrough.class, Parameters.parseString("{\"name\":\"three\", \"connIn\" : [\"conn-2-x\"], \"connOut\" : [\"conn-3-5\"]}")));
    job.add(three);

    Stage four = new Stage("four");
    four.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-2-x", new TupleflowString.ValueOrder()));
    four.add(new StageConnectionPoint(ConnectionPointType.Output,
            "conn-4-5", new TupleflowString.ValueOrder()));
    four.add(new Step(PassThrough.class, Parameters.parseString("{\"name\":\"four\", \"connIn\" : [\"conn-2-x\"], \"connOut\" : [\"conn-4-5\"]}")));
    job.add(four);


    Stage five = new Stage("five");
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-3-5", new TupleflowString.ValueOrder()));
    five.add(new StageConnectionPoint(ConnectionPointType.Input,
            "conn-4-5", new TupleflowString.ValueOrder()));
    // two generates 10 items per instance (20)
    // the items are sent to three (combined - 20)
    // the items are sent to four (each - 20)
    // three and four send their items to five
    // should recieve 40 total
    five.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":40, \"connIn\" : [\"conn-3-5\",\"conn-4-5\"]}")));
    job.add(five);

    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("two", "three", ConnectionAssignmentType.Combined);
    job.connect("two", "four", ConnectionAssignmentType.Each);
    job.connect("three", "five", ConnectionAssignmentType.Combined);
    job.connect("four", "five", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);

    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testSingleSingleCombGZIP() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");
    one.addOutput("conn-1-2", new TupleflowString.ValueOrder(), CompressionType.GZIP);
    one.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"one\", \"conn\":[\"conn-1-2\"]}")));
    job.add(one);

    Stage two = new Stage("two");
    two.addInput("conn-1-2", new TupleflowString.ValueOrder());
    // should recieve 10 items from one
    two.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":10, \"connIn\" : [\"conn-1-2\"]}")));
    job.add(two);

    job.connect("one", "two", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);
    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  public void testSingleMultiEachGZIP() throws Exception {
    Job job = new Job();

    Stage one = new Stage("one");

    one.addOutput("conn-1-2", new TupleflowString.ValueOrder(), CompressionType.GZIP);
    one.add(new Step(NullSource.class));
    job.add(one);

    Stage two = new Stage("two");
    two.addInput("conn-1-2", new TupleflowString.ValueOrder());
    two.addOutput("conn-2-3", new TupleflowString.ValueOrder(), CompressionType.GZIP);
    two.add(new Step(Generator.class, Parameters.parseString("{\"name\":\"two\", \"conn\":[\"conn-2-3\"]}")));
    job.add(two);

    Stage three = new Stage("three");
    three.addInput("conn-2-3", new TupleflowString.ValueOrder());
    // should recieve 10 items from each instance of two (20 total)
    three.add(new Step(Receiver.class, Parameters.parseString("{\"expectedCount\":20, \"connIn\" : [\"conn-2-3\"]}")));
    job.add(three);

    job.connect("one", "two", ConnectionAssignmentType.Each);
    job.connect("two", "three", ConnectionAssignmentType.Combined);

    job.properties.put("hashCount", "2");
    ErrorStore err = new ErrorStore();
    Verification.verify(job, err);
    JobExecutor.runLocally(job, err, Parameters.parseString("{\"server\":false}"));
    if (err.hasStatements()) {
      throw new RuntimeException(err.toString());
    }
  }

  ///***** Classes used to generate/pass/merge/receive data ******///
  public static class NullSource implements ExNihiloSource {

    @Override
    public void run() throws IOException {
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
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

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
      if (!parameters.getJSON().isString("name")) {
        store.addError("Generator - Could not find the name of the stage in parameters");
      }
      if (!parameters.getJSON().isList("conn", Type.STRING)) {
        store.addError("Generator - Could not find any connections specified in parameters");
      }
      for (String conn : (List<String>) parameters.getJSON().getList("conn")) {
        if (!parameters.writerExists(conn, TupleflowString.class.getName(), TupleflowString.ValueOrder.getSpec())) {
          store.addError("Generator - Could not verify connection: " + conn);
        }
      }
    }
  }

  public static class PassThrough implements ExNihiloSource {

    TupleFlowParameters params;
    String suffix;

    public PassThrough(TupleFlowParameters params) {
      this.params = params;
      String name = params.getJSON().getString("name");
      suffix = String.format("-(%s-%d)", name, params.getInstanceId());
    }

    @Override
    public void run() throws IOException {
      List<String> connIn = (List<String>) params.getJSON().getList("connIn");
      List<String> connOut = (List<String>) params.getJSON().getList("connOut");

      for (int i = 0; i < connIn.size(); i++) {
        TypeReader<TupleflowString> reader = params.getTypeReader(connIn.get(i));
        Processor<TupleflowString> writer = params.getTypeWriter(connOut.get(i));
        TupleflowString obj = reader.read();
        while (obj != null) {
          obj.value += suffix;
          writer.process(obj);
          obj = reader.read();
        }
        writer.close();
      }
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
      if (!parameters.getJSON().isString("name")) {
        store.addError("PassThrough - Could not find the name of the stage in parameters");
      }
      if (!parameters.getJSON().isList("connIn", Type.STRING)) {
        store.addError("PassThrough - Could not find any Input connections specified in parameters");
      }
      if (!parameters.getJSON().isList("connOut", Type.STRING)) {
        store.addError("PassThrough - Could not find any Output connections specified in parameters");
      }
    }
  }

  public static class Merge implements ExNihiloSource {

    TupleFlowParameters params;
    String suffix;

    public Merge(TupleFlowParameters params) {
      this.params = params;
      String name = params.getJSON().getString("name");
      suffix = String.format("-(%s-%d)", name, params.getInstanceId());
    }

    @Override
    public void run() throws IOException {
      List<String> connIn = (List<String>) params.getJSON().getList("connIn");
      String connOut = params.getJSON().getString("connOut");
      Processor<TupleflowString> writer = params.getTypeWriter(connOut);
      Sorter sorter = new Sorter(new TupleflowString.ValueOrder(), null, writer);

      for (int i = 0; i < connIn.size(); i++) {
        TypeReader<TupleflowString> reader = params.getTypeReader(connIn.get(i));
        TupleflowString obj = reader.read();
        while (obj != null) {
          obj.value += suffix;
          sorter.process(obj);
          obj = reader.read();
        }
      }
      sorter.close();
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
      if (!parameters.getJSON().isString("name")) {
        store.addError("PassThrough - Could not find the name of the stage in parameters");
      }
      if (!parameters.getJSON().isList("connIn", Type.STRING)) {
        store.addError("PassThrough - Could not find any Input connections specified in parameters");
      }
      if (!parameters.getJSON().isString("connOut")) {
        store.addError("PassThrough - Could not find an Output connection specified in parameters");
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
      int counter = 0;
      List<String> connIn = (List<String>) params.getJSON().getList("connIn");
      for (int i = 0; i < connIn.size(); i++) {
        TypeReader<TupleflowString> reader = params.getTypeReader(connIn.get(i));
        TupleflowString obj = reader.read();
        while (obj != null) {
          // DEBUGGING //
          // System.err.println("REC - " + params.getInstanceId() + " : " + obj.value);
          counter++;
          obj = reader.read();
        }
      }
      if (counter != params.getJSON().getLong("expectedCount")) {
        throw new IOException("Did not receive the expected number of items.");
      }
    }

    @Override
    public void setProcessor(org.lemurproject.galago.tupleflow.Step processor) throws IncompatibleProcessorException {
      Linkage.link(this, processor);
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) throws IOException {
      if (!parameters.getJSON().isList("connIn", Type.STRING)) {
        store.addError("PassThrough - Could not find any Input connections specified in parameters");
      }
    }
  }
}
