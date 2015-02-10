package org.lemurproject.galago.core.parse.tagtok;

import java.util.Map;

/**
 * @author trevor.
 * @see org.lemurproject.galago.core.parse.TagTokenizer
 */
public final class BeginTag {

	public BeginTag(String name, Map<String, String> attributes, int bytePosition, int end) {
		this.name = name;
		this.attributes = attributes;

		this.bytePosition = bytePosition;
		this.termPosition = end;
	}
	public String name;
	public Map<String, String> attributes;
	public int bytePosition;
	public int termPosition;
}
