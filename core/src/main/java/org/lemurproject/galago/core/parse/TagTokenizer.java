// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.tagtok.*;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StringPooler;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * <p>This class processes document text into tokens that can be indexed.</p>
 * 
 * <p>The text is assumed to contain some HTML/XML tags.  The tokenizer tries
 * to extract as much data as possible from each document, even if it is not
 * well formed (e.g. there are start tags with no ending tags).  The resulting
 * document object contains an array of terms and an array of tags.</p>
 *
 * This class is <strong>NOT</strong> threadsafe.
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class TagTokenizer extends Tokenizer {
  public static final Logger log = Logger.getLogger(TagTokenizer.class.getName());
	public static HashSet<String> ignoredTags = new HashSet<>(Arrays.asList("script", "style"));

  protected List<Pattern> whitelist;
	public TagTokenizerParser state;

	public TagTokenizer() {
		this(Parameters.create());
	}
	public TagTokenizer(Parameters tokenizerParameters) {
		this(new FakeParameters(tokenizerParameters));
	}
	public TagTokenizer(TupleFlowParameters parameters) {
		super(parameters);
		init(parameters.getJSON());
  }

	private void init(Parameters argp) {
		state = new TagTokenizerParser(argp);

		whitelist = new ArrayList<>();
		// This has to come after we initialize whitelist.
		if (argp.isList("fields") || argp.isString("fields")) {
			for (String value : argp.getAsList("fields", String.class)) {
				assert(whitelist != null);
				addField(value);
			}
		}
	}


	/** Register the fields that should be parsed and collected */
	public void addField(String f) {
    whitelist.add(Pattern.compile(f));
  }

	/**
   * Resets parsing in preparation for the next document.
   */
  public void reset() {
		state.reset();
  }

	/**
   * Parses the state.text in the document.state.text attribute and fills in the
   * document.terms and document.tags arrays.
   *
   */
  @Override
  public void tokenize(Document document) {
    reset();
    assert(document != null);
    state.text = document.text;
    assert(state.text != null);

    try {
			state.parse();
			// Pull tag information into this document object.
			state.finishDocument(document, whitelist);
		} catch (Exception e) {
      log.log(Level.WARNING, "Parse failure: " + document.name, e);
    }

		assert(document.terms != null);
		StringPooler.getInstance().transform(document.terms);
  }

	public ArrayList<IntSpan> getTokenPositions() {
    return state.tokenPositions;
  }
}
