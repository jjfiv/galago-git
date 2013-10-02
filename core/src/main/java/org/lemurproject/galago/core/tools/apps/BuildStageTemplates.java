// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.index.disk.FieldIndexWriter;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.parse.DocumentNumberer;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.core.parse.UniversalParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.core.types.NumberedField;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionPointType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.StageConnectionPoint;
import org.lemurproject.galago.tupleflow.execution.Step;

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
      Utility.makeParentDirectories(manifest);
      Utility.copyStringToFile(jobP.toPrettyString(), manifest);
  }

  public static Stage getGenericWriteStage(String stageName, File destination, String inputPipeName,
          Class<? extends org.lemurproject.galago.tupleflow.Step> writer, Order dataOrder, Parameters p) throws IOException {
    Stage stage = new Stage(stageName);
    stage.add(new StageConnectionPoint(ConnectionPointType.Input,
            inputPipeName, dataOrder));
    p.set("filename", destination.getCanonicalPath());
    stage.add(new InputStep(inputPipeName));
    stage.add(new Step(writer, p));
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
    stage.add(new InputStep(inputs[0]));
    stage.add(new Step(writer, p));
    return stage;
  }

  public static ArrayList<Step> getExtractionSteps(String outputName, Class extractionClass, Order sortOrder) {
    ArrayList<Step> steps = new ArrayList<Step>();
    steps.add(new Step(extractionClass));
    steps.add(Utility.getSorter(sortOrder));
    steps.add(new OutputStep(outputName));
    return steps;
  }

  public static ArrayList<Step> getExtractionSteps(String outputName, Class extractionClass, Parameters p, Order sortOrder) {
    ArrayList<Step> steps = new ArrayList<Step>();
    steps.add(new Step(extractionClass, p));
    steps.add(Utility.getSorter(sortOrder));
    steps.add(new OutputStep(outputName));
    return steps;
  }

  /**
   * Writes document lengths to a document lengths file.
   */
  public static Stage getWriteLengthsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getWriteLengthsStage(stageName, destination, inputPipeName, new Parameters());
  }

  public static Stage getWriteLengthsStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    p.set("blockSize", 512);
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DiskLengthsWriter.class, new FieldLengthData.FieldDocumentOrder(), p);
  }

  /**
   * Writes document names to a document names file.
   */
  public static Stage getWriteNamesStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getWriteNamesStage(stageName, destination, inputPipeName, new Parameters());
  }

  public static Stage getWriteNamesStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    p.set("blockSize", 512);
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DiskNameWriter.class, new NumberedDocumentData.NumberOrder(), p);
  }
  
  public static Stage getWriteNamesRevStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getWriteNamesRevStage(stageName, destination, inputPipeName, new Parameters());
  }

  public static Stage getWriteNamesRevStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    p.set("blockSize", 512);
    return getGenericWriteStage(stageName, destination, inputPipeName,
            DiskNameReverseWriter.class, new NumberedDocumentData.IdentifierOrder(), p);
  }

  public static Stage getWriteExtentsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            WindowIndexWriter.class, new NumberedExtent.ExtentNameNumberBeginOrder(), new Parameters());
  }

  public static Stage getWriteExtentsStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            WindowIndexWriter.class, new NumberedExtent.ExtentNameNumberBeginOrder(), p);
  }

  public static Stage getWriteFieldsStage(String stageName, File destination, String inputPipeName) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            FieldIndexWriter.class, new NumberedField.FieldNameNumberOrder(), new Parameters());
  }

  public static Stage getWriteFieldsStage(String stageName, File destination, String inputPipeName, Parameters p) throws IOException {
    return getGenericWriteStage(stageName, destination, inputPipeName,
            FieldIndexWriter.class, new NumberedField.FieldNameNumberOrder(), p);
  }

  public static Stage getSplitStage(List<String> inputPaths, Class sourceClass) throws IOException {
    return getSplitStage(inputPaths, sourceClass, new DocumentSplit.FileIdOrder(), new Parameters());
  }

  public static Stage getSplitStage(List<String> inputPaths, Class sourceClass, Parameters p) throws IOException {
    return getSplitStage(inputPaths, sourceClass, new DocumentSplit.FileIdOrder(), p);
  }
  
  public static Stage getSplitStage(List<String> inputPaths, Class sourceClass, Order order, Parameters p)
          throws IOException {
    Stage stage = new Stage("inputSplit");
    stage.addOutput("splits", order);

    List<String> inputFiles = new ArrayList<String>();
    List<String> inputDirectories = new ArrayList<String>();
    for (String input : inputPaths) {
      File inputFile = new File(input);

      if (inputFile.isFile()) {
        inputFiles.add(inputFile.getAbsolutePath());
      } else if (inputFile.isDirectory()) {
        inputDirectories.add(inputFile.getAbsolutePath());
      } else {
        throw new IOException("Couldn't find file/directory: " + input);
      }
      p.set("filename", inputFiles);
      p.set("directory", inputDirectories);
    }

    stage.add(new Step(sourceClass, p));
    stage.add(Utility.getSorter(order));
    stage.add(new OutputStep("splits"));
    return stage;
  }

  public static Step getNumberingStep(Parameters p) {
      return getNumberingStep(p, DocumentNumberer.class);
  }

  public static Step getNumberingStep(Parameters p, Class defaultClass) {
    return getGenericStep("numberer", p, defaultClass);
  }

  public static Step getParserStep(Parameters p) {
    return getParserStep(p, UniversalParser.class);
  }

  public static Step getParserStep(Parameters p, Class defaultClass) {
    return getGenericStep("parser", p, defaultClass);
  }

  public static Step getStemmerStep(Parameters p, Class defaultClass) {
    return getGenericStep("stemmer", p, defaultClass);
  }

  public static Step getTokenizerStep(Parameters p) {
    return getTokenizerStep(p, Tokenizer.getTokenizerClass(p));
  }

  public static Step getTokenizerStep(Parameters p, Class defaultClass) {
    return getGenericStep("tokenizer", p, defaultClass);
  }

  public static Step getGenericStep(String stepname, Parameters p, Class defaultClass) {
    if (p == null || !p.isMap(stepname)) {
      return new Step(defaultClass);
    }

    Parameters stepParams = p.getMap(stepname);

    // Try to get the stepParams class specified - use default otherwise
    Class stepClass = null;
    String stepClassName = null;
    try {
      stepClassName = stepParams.get("class", defaultClass.getName());
      stepClass = Class.forName(stepClassName);
    } catch (ClassNotFoundException cnfe) {
      System.err.printf("WARNING: Step class %s cound not be found: %s\n",
              stepClassName, cnfe.getMessage());
    }

    // Pull out any parameters under the stepParams class name
    // (this parameterizes defaults as well)

    // Return stepParams encapsulating the class and params
    return new Step(stepClass, stepParams);
  }
}
