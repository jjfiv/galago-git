package org.lemurproject.galago.core.parse.tagtok;

import org.lemurproject.galago.core.parse.Document;

import java.util.List;
import java.util.regex.Pattern;

/**
* @author jfoley.
*/
public interface DocumentBuilder {
	/**
	 * Adds a token to the document object.
	 *
	 * @param token  The token to add.
	 * @param start  The starting byte offset of the token in the document text.
	 * @param end    The ending byte offset of the token in the document text.
	 */
	public void addToken(final String token, int start, int end);

	/** Set fields on document object. */
	public void finishDocument(Document doc, List<Pattern> tagWhitelist);
}
