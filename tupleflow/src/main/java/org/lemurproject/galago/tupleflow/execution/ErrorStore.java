// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * @author trevor
 */
public class ErrorStore {

  public static class Statement implements Comparable<Statement> {

    public Statement(String message) {
      this.location = "unknown";
      this.message = message;
    }
    
    public Statement(String fileName, String message) {
      this.location = fileName;
      this.message = message;
    }

    public String toString(String messageType) {
      String result;

      if (location == null) {
        result = String.format("[unknown location] %s: %s\n", messageType,
                message);
      } else {
        result = String.format("%s [%s]: %s\n", location, messageType, message);
      }
      return result;
    }

    public String toString() {
      return toString("INFO");
    }

    public int compareTo(Statement other) {
      if (location == null) {
        if (other.location == null) {
          return 0;
        } else {
          return -1;
        }
      } else {
        if (other.location == null) {
          return 1;
        } else {
          return location.compareTo(other.location);
        }
      }
    }
    String location;
    String message;
  }

  ArrayList<Statement> errors = new ArrayList();
  ArrayList<Statement> warnings = new ArrayList();

  public void addError(String message) {
    errors.add(new Statement(message));
  }
  
  public void addError(String location, String message) {
    errors.add(new Statement(location, message));
  }

  public void addWarning(String location, String message) {
    warnings.add(new Statement(location, message));
  }
  
  public void addWarning(String message) {
    warnings.add(new Statement(message));
  }

  public ArrayList<Statement> getErrors() {
    return errors;
  }

  public ArrayList<Statement> getWarnings() {
    return warnings;
  }

  public boolean hasStatements() {
    return errors.size() + warnings.size() > 0;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    Collections.sort(errors);
    Collections.sort(warnings);

    for (Statement s : errors) {
      builder.append(s.toString("Error"));
    }

    for (Statement s : warnings) {
      builder.append(s.toString("Warning"));
    }

    return builder.toString();
  }
}
