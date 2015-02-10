package org.lemurproject.galago.core.parse.tagtok;

/**
 * @author jfoley.
 */
public class TagPunctuation {

	static final char[] splitChars = {
	  ' ', '\t', '\n', '\r', // spaces
		';', '\"', '&', '/', ':', '!', '#',
		'?', '$', '%', '(', ')', '@', '^',
		'*', '+', '-', ',', '=', '>', '<', '[',
		']', '{', '}', '|', '`', '~', '_'
	};

	public static final boolean[] splits;
	static {
		splits = TagPunctuation.buildSplits();
	}

	public static boolean[] buildSplits() {
		boolean[] localSplits = new boolean[257];

		for (int i = 0; i < localSplits.length; i++) {
			localSplits[i] = false;
		}

		for (char c : splitChars) {
			localSplits[(byte) c] = true;
		}

		for (byte c = 0; c <= 32; c++) {
			localSplits[c] = true;
		}

		return localSplits;
	}
}
