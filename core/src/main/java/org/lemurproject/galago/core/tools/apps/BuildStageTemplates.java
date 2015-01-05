// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.disk.*;
import org.lemurproject.galago.core.parse.DocumentNumberer;
import org.lemurproject.galago.core.parse.UniversalParser;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.core.types.*;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.*;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author irmarc
 */
public class BuildStageTemplates {

  // Cannot instantiate - just a container class
  private BuildStageTemplates() {
  }

  public static void writeManifest(String indexPath, Parameters jobP) throws IOException {
      File manifest = new File(indexPath, "buildManifest.json");
      FSUtil.makeParentDirectories(manifest);
      StreamUtil.copyStringToFile(jobP.toPrettyString(), manifest);
  }

  public static Stage getGenericWriteStage(String stageName, File destination, String inputPipeName,
          Class<? extends org.lemurproject.galago.tupleflow.Step> writer, Order dataOrder, Parameters p) throws IOException {
    Stage stage = new Stage(stageName);
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            inputPipeName, dataOrder));
    p.set("filename", destination.getCanonicalPath());
    stage.add(new InputStepInformation(inputPipeName));
    stage.add(new StepInformation(writer, p));
    return stage;
  }

  public static Stage getGenericWriteStage(String stageName, File destination, String[] inputs,
           Order[] orders, Class<? extends org.lemurproject.galago.tupleflow.Step> writer, Parameters p) throws IOException {
    Stage stage = new Stage(stageName);
    assert(inputs.length == orders.length);
    for (int i = 0; i < inputs.length; i++) {
      stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            inputs[i], orders[i]));
    }
    p.set("filename", destination.getCanonicalPath());
    stage.add(new InputStepInformation(inputs[0]));
    stage.add(new StepInformation(writer, p));
    return stage;
  }

  public static ArrayList<StepInformation> getExtractionSteps(String outputName, Class extractionClass, Order sortOrder) {
    ArrayList<StepInformation> steps = new ArrayList<StepInformation>();
    steps.add(new StepInformation(extractionClass));
    steps.add(Utility.getSorter(sortOrder));
    steps.add(new OutputStepInformation(outputName));
    return steps;
  }

  public static ArrayList<StepInformation> getExtractionSteps(String outputName, Class extractionClass, Parameters p, Order sortOrder) {
    ArrayList<StepInformation> steps = new ArrayList<StepInformation>();
    steps.add(new StepInformation(extractionClass, p));
    steps.add(Utility.getSorter(sortOrder));
    steps.add(new OutputStepInformation(outputName));
    return steps;
  }

  /**
   * Writes document lengths to a document lengths file.
   */
  public static Stage getWriteLengthsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getWriteLengthsStage(stageName, destination, inputPipeName, Parameters.create());
  }

  public static Stage getWriteLengthsStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    // this is the factor by which we want to pack in fields...
    p.setIfMissing("blockSize", 4096);
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DiskLengthsWriter.class, new FieldLengthData.FieldDocumentOrder(), p);
  }

  /**
   * Writes document names to a document names file.
   */
  public static Stage getWriteNamesStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getWriteNamesStage(stageName, destination, inputPipeName, Parameters.create());
  }

  public static Stage getWriteNamesStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    p.setIfMissing("blockSize", 4096);
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DiskNameWriter.class, new DocumentNameId.IdOrder(), p);
  }
  
  public static Stage getWriteNamesRevStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getWriteNamesRevStage(stageName, destination, inputPipeName, Parameters.create());
  }

  public static Stage getWriteNamesRevStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    p.setIfMissing("blockSize", 4096);
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DiskNameReverseWriter.class, new DocumentNameId.NameOrder(), p);
  }

  public static Stage getWriteExtentsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            WindowIndexWriter.class, new NumberedExtent.ExtentNameNumberBeginOrder(), Parameters.create());
  }

  public static Stage getWriteExtentsStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            WindowIndexWriter.class, new NumberedExtent.ExtentNameNumberBeginOrder(), p);
  }

  public static Stage getWriteFieldsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            FieldIndexWriter.class, new NumberedField.FieldNameNumberOrder(), Parameters.create());
  }

  public static Stage getWriteFieldsStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            FieldIndexWriter.class, new NumberedField.FieldNameNumberOrder(), p);
  }

  public static Stage getSplitStage(List<String> inputPaths, Class sourceClass) throws IOException {
    return getSplitStage(inputPaths, sourceClass, new DocumentSplit.FileIdOrder(), Parameters.create());
  }

  public static Stage getSplitStage(List<String> inputPaths, Class sourceClass, Parameters p) throws IOException {
    return getSplitStage(inputPaths, sourceClass, new DocumentSplit.FileIdOrder(), p);
  }
  
  public static Stage getSplitStage(List<String> inputPaths, Class sourceClass, Order order, Parameters p)
          throws IOException {
    Stage stage = new Stage("inputSplit");
    stage.addOutput("splits", order);

    // check paths
    for (String input : inputPaths) {
      File inputFile = new File(input);

      if (!inputFile.isFile() && !inputFile.isDirectory()) {
        throw new IOException("Couldn't find file/directory: " + input);
      }
    }
    p.put("inputPath", inputPaths);

    stage.add(new StepInformation(sourceClass, p));
    stage.add(Utility.getSorter(order));
    stage.add(new OutputStepInformation("splits"));
    return stage;
  }

  public static StepInformation getNumberingStep(Parameters p) {
      return getNumberingStep(p, DocumentNumberer.class);
  }

  public static StepInformation getNumberingStep(Parameters p, Class defaultClass) {
    return getGenericStep("numberer", p, defaultClass);
  }

  public static StepInformation getParserStep(Parameters p) {
    return getParserStep(p, UniversalParser.class);
  }

  public static StepInformation getParserStep(Parameters p, Class defaultClass) {
    return getGenericStep("parser", p, defaultClass);
  }

  public static StepInformation getStemmerStep(Parameters p, Class defaultClass) {
    return getGenericStep("stemmer", p, defaultClass);
  }

  public static StepInformation getTokenizerStep(Parameters p) {
    return getTokenizerStep(p, Tokenizer.getTokenizerClass(p));
  }

  public static StepInformation getTokenizerStep(Parameters p, Class defaultClass) {
    return getGenericStep("tokenizer", p, defaultClass);
  }

  public static StepInformation getGenericStep(String stepname, Parameters p, Class defaultClass) {
    if (p == null || !p.isMap(stepname)) {
      return new StepInformation(defaultClass);
    }

    Parameters stepParams = p.getMap(stepname);

    // Try to get the stepParams class specified - use default otherwise
    Class stepClass;
    String stepClassName = null;
    try {
      stepClassName = stepParams.get("class", defaultClass.getName());
      stepClass = Class.forName(stepClassName);
    } catch (ClassNotFoundException cnfe) {
      System.err.printf("WARNING: Step class %s cound not be found: %s\n",
              stepClassName, cnfe.getMessage());
      throw new RuntimeException(cnfe);
    }

    // Pull out any parameters under the stepParams class name
    // (this parameterizes defaults as well)

    // Return stepParams encapsulating the class and params
    return new StepInformation(stepClass, stepParams);
  }
}
