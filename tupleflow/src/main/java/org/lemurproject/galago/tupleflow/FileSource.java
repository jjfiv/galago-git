// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 *
 * @author trevor
 */
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.FileName", order = {"+filename"})
public class FileSource implements ExNihiloSource<FileName> {
    TupleFlowParameters parameters;
    public Processor<FileName> processor;

    /** Creates a new instance of FileSource */
    public FileSource(TupleFlowParameters parameters) {
        this.parameters = parameters;
    }

    private void processDirectory(File root) throws IOException {
        for (File file : root.listFiles()) {
            if (file.isHidden()) {
                continue;
            }
            if (file.isDirectory()) {
                processDirectory(file);
            } else {
                processor.process(new FileName(file.toString()));
            }
        }
    }

    public void run() throws IOException {
        if (parameters.getJSON().containsKey("directory")) {
            List<String> directories = parameters.getJSON().getList("directory");

            for (String directory : directories) {
                File directoryFile = new File(directory);
                processDirectory(directoryFile);
            }
        }
        if (parameters.getJSON().containsKey("filename")) {
            List<String> files = parameters.getJSON().getList("filename");

            for (String file : files) {
                processor.process(new FileName(file));
            }
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

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        if (!(parameters.getJSON().containsKey("directory") || parameters.getJSON().containsKey("filename"))) {
            handler.addError("FileSource requires either at least one directory or filename parameter.");
            return;
        }

        if (parameters.getJSON().containsKey("directory")) {
            List<String> directories = parameters.getJSON().getList("directory");

            for (String directory : directories) {
                File directoryFile = new File(directory);

                if (directoryFile.exists() == false) {
                    handler.addError("Directory " + directoryFile.toString() + " doesn't exist.");
                } else if (directoryFile.isDirectory() == false) {
                    handler.addError(directoryFile.toString() + " exists, but it isn't a directory.");
                }
            }
        }
        if (parameters.getJSON().containsKey("filename")) {
            List<String> files = parameters.getJSON().getList("filename");

            for (String file : files) {
                File f = new File(file);

                if (f.exists() == false) {
                    handler.addError("File " + file + " doesn't exist.");
                } else if (f.isFile() == false) {
                    handler.addError(file + " exists, but isn't a file.");
                }
            }
        }
    }
}
