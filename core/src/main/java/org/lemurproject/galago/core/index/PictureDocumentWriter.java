// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/** 
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class PictureDocumentWriter extends KeyValueWriter<KeyValuePair> {
    public PictureDocumentWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
	super(parameters, "Pic Documents Written");
	Parameters manifest = writer.getManifest();
	manifest.set("writerClass", this.getClass().getName());
    }

    public GenericElement prepare(KeyValuePair kvp) {
	return new GenericElement(kvp.key, kvp.value);
    } 
}
