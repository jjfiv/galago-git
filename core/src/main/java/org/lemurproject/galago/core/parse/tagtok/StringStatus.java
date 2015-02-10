package org.lemurproject.galago.core.parse.tagtok;

/**
 * @author trevor.
 * @see org.lemurproject.galago.core.parse.TagTokenizer
 */
public enum StringStatus {
	Clean,
	NeedsSimpleFix,
	NeedsComplexFix,
	NeedsAcronymProcessing
}
