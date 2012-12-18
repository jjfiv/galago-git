// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.ByteArrayOutputStream;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import java.io.IOException;
import java.text.ParseException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.HashMap;
import org.lemurproject.galago.core.types.NumberedField;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Converts all tags from a document object into DocumentField tuples.
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberedField")
@Verified
public class NumberedFieldExtractor extends StandardStep<Document, NumberedField> {

  HashMap<String, String> trackedFields = new HashMap<String, String>();

  public NumberedFieldExtractor(TupleFlowParameters parameters) {
    if (parameters.getJSON().containsKey("tokenizer")) {
      Parameters tokenizerParams = parameters.getJSON().getMap("tokenizer");
      if (tokenizerParams.isMap("formats")) {
        Parameters formats = tokenizerParams.getMap("formats");
        for (String field : formats.getKeys()) {
          trackedFields.put(field, formats.getString(field));
        }
      }
    }
  }

  @Override
  public void process(Document document) throws IOException {
    for (Tag tag : document.tags) {
      if (trackedFields.containsKey(tag.name)) {
        String stringForm;
        if(tag.charBegin < 0){
          StringBuilder sb = new StringBuilder();
          for (int i = tag.begin; i < tag.end; i++) {
            sb.append(document.terms.get(i)).append(" ");
          }
          stringForm = sb.toString();
        } else {
          stringForm = document.text.substring(tag.charBegin, tag.charEnd);
        }

        byte[] bytes = getByteData(tag.name, stringForm.trim());
        processor.process(new NumberedField(Utility.fromString(tag.name),
                document.identifier, bytes));
      }
    }
  }

  private byte[] getByteData(String tag, String stringForm) throws IOException {
    String format = trackedFields.get(tag);
    if (format.equals("string")) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] strBytes = Utility.fromString(stringForm);
      baos.write(Utility.compressInt(strBytes.length));
      baos.write(strBytes);
      return baos.toByteArray();
    } else if (format.equals("int")) {
      return Utility.fromInt(Integer.parseInt(stringForm));
    } else if (format.equals("long")) {
      return Utility.fromLong(Long.parseLong(stringForm));
    } else if (format.equals("float")) {
      return Utility.fromInt(Float.floatToIntBits(Float.parseFloat(stringForm)));
    } else if (format.equals("double")) {
      return Utility.fromLong(Double.doubleToLongBits(Double.parseDouble(stringForm)));
    } else if (format.equals("date")) {
      try {
	// See if it fits a year
	if (stringForm.length() == 4) {
	    Calendar cal = Calendar.getInstance();
	    cal.set(Integer.parseInt(stringForm), 1, 1);
	    return Utility.fromLong(cal.getTimeInMillis());				   
	}

	// More generic
        DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
        return Utility.fromLong(df.parse(stringForm).getTime());
      } catch (ParseException pe) {
        throw new IOException(pe);
      }
    } else {
      throw new IOException(String.format("Don't have any plausible format for tag %s\n", tag));
    }
  }
}
