// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.File;
import java.io.Serializable;

/**
 * A data pipe carries tuples from m sources/outputs to n sinks/outputs.
 *
 * @author trevor
 */
;

public class DataPipe implements Serializable {

  public DataPipe(String root, String pipeName, String className, String[] order, String[] hash, int inputCount, int outputCount) {
    this.root = root;
    this.pipeName = pipeName;
    this.className = className;
    this.order = order;
    this.hash = hash;
    this.setInputCount(inputCount);
    this.setOutputCount(outputCount);
  }

  public String getPipeName() {
    return pipeName;
  }

  @Override
  public String toString() {
    String out = String.format("className: %s\n", className);
    out += "Pipe path: " + root + "\n";
    out += getInputCount() + "\n";
    out += getOutputCount() + "\n";

    out += "order: [" + order.length + "]/";

    for (String o : order) {
      out += o + "/";
    }
    out += "\n";

    out += "hash: [" + hash.length + "]/";
    for (String h : hash) {
      out += h + "/";
    }
    out += "\n";
    return out;
  }

  public String[] getInputFileNames(int index) {
    String[] inputNames = null;

    if (hash != null) {
      inputNames = new String[getOutputCount()];

      for (int i = 0; i < getOutputCount(); i++) {
        inputNames[i] = getFileName(index, i);
      }
    } else {
      inputNames = new String[]{getFileName(index, index)};
    }

    return inputNames;
  }

  public String[] getOutputFileNames(int index) {
    String[] outputNames = null;

    if (hash != null) {
      outputNames = new String[getInputCount()];

      for (int i = 0; i < getInputCount(); i++) {
        outputNames[i] = getFileName(i, index);
      }
    } else {
      outputNames = new String[]{getFileName(index, index)};
    }

    return outputNames;
  }

  public String getFileName(int inputIndex, int outputIndex) {
    return root + File.separator + inputIndex + File.separator + outputIndex;
  }

  public void makeDirectories() {
    for (int i = 0; i < getInputCount(); i++) {
      new File(root + File.separator + i).mkdirs();
    }
  }

  public int getInputCount() {
    return inputCount;
  }

  public void setInputCount(int inputCount) {
    this.inputCount = inputCount;
  }

  public int getOutputCount() {
    return outputCount;
  }

  public void setOutputCount(int outputCount) {
    this.outputCount = outputCount;
  }

  public String[] getHash() {
    return hash;
  }

  public String getClassName() {
    return className;
  }

  public String[] getOrder() {
    return order;
  }
  public String root;
  public String pipeName;
  private int inputCount;
  private int outputCount;
  public String className;
  public String[] order;
  public String[] hash;
}
