// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.types.FileName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author trevor
 */
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.FileName", order = {"+filename"})
public class FileSource implements ExNihiloSource<FileName> {

  TupleFlowParameters parameters;
  public Processor<FileName> processor;

  /**
   * Creates a new instance of FileSource
   */
  public FileSource(TupleFlowParameters parameters) {
    this.parameters = parameters;
  }

  private void processRecursively(File root) throws IOException {
    if (root.isFile() && !root.isHidden()) {
      processor.process(new FileName(root.toString()));
    } else {
      for (File file : FileUtility.safeListFiles(root)) {
        processRecursively(file);
      }
    }
  }

  @Override
  public void run() throws IOException {
    List<String> inputs = new ArrayList<String>();
    if (parameters.getJSON().containsKey("input")) {
      inputs.addAll(parameters.getJSON().getAsList("input", String.class));
    }
    if (parameters.getJSON().containsKey("directory")) {
      inputs.addAll(parameters.getJSON().getAsList("directory", String.class));
    }
    if (parameters.getJSON().containsKey("filename")) {
      inputs.addAll(parameters.getJSON().getAsList("filename", String.class));
    }
    for (String input : inputs) {
      processRecursively(new File(input));
    }
    processor.close();
  }

  public void close() throws IOException {
    processor.close();
  }

  @Override
  public void setProcessor(Step nextStage) throws IncompatibleProcessorException {
    Linkage.link(this, nextStage);
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!(parameters.getJSON().containsKey("directory") || parameters.getJSON().containsKey("filename")
            || parameters.getJSON().containsKey("input"))) {
      store.addError("FileSource requires either at least one directory or filename parameter.");
      return;
    }

    if (parameters.getJSON().containsKey("directory")) {
      List<String> directories = parameters.getJSON().getList("directory", String.class);

      for (String directory : directories) {
        File directoryFile = new File(directory);

        if (!directoryFile.exists()) {
          store.addError("Directory " + directoryFile.toString() + " doesn't exist.");
        }
      }
    }
    if (parameters.getJSON().containsKey("filename")) {
      List<String> files = parameters.getJSON().getList("filename", String.class);

      for (String file : files) {
        File f = new File(file);

        if (!f.exists()) {
          store.addError("File " + file + " doesn't exist.");
        }
      }
    }
  }
}
