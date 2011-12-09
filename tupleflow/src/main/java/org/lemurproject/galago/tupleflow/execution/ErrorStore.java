// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.Collections;
import org.xml.sax.SAXParseException;

/**
 *
 * @author trevor
 */
public class ErrorStore {
    public static class Statement implements Comparable<Statement> {
        public Statement(FileLocation location, String message) {
            this.location = location;
            this.message = message;
        }

        public String toString(String messageType) {
            String result;

            if (location == null) {
                result = String.format("[unknown location] %s: %s\n", messageType,
                                                         message);
            } else {
                result = String.format("%s [Line %d Column %d] %s: %s\n", location.fileName,
                                          location.lineNumber, location.columnNumber, messageType,
                                          message);
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
                return location.compareTo(other.location);
            }
        }
        FileLocation location;
        String message;
    }

    public class LocatedHandler implements ErrorHandler {
        public LocatedHandler(FileLocation location) {
            this.location = location;
        }

        public void addError(String message) {
            ErrorStore.this.addError(location, message);
        }

        public void addWarning(String message) {
            ErrorStore.this.addWarning(location, message);
        }
        FileLocation location;
    }
    ArrayList<Statement> errors = new ArrayList();
    ArrayList<Statement> warnings = new ArrayList();

    public void addError(FileLocation location, String message) {
        errors.add(new Statement(location, message));
    }

    public void addWarning(FileLocation location, String message) {
        warnings.add(new Statement(location, message));
    }

    public LocatedHandler getErrorHandler(FileLocation location) {
        return new LocatedHandler(location);
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

    void addError(String filename, SAXParseException e) {
        addError(new FileLocation(filename, e.getLineNumber(), e.getColumnNumber()), e.getMessage());
    }
}
