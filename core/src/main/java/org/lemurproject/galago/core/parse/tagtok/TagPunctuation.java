package org.lemurproject.galago.core.parse.tagtok;

/**
 * @author trevor.
 * @see org.lemurproject.galago.core.parse.TagTokenizer
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

	public static String clean(String input) {
		StringBuilder output = new StringBuilder();
		for (char c : input.toCharArray()) {
			if(c < splits.length && splits[c]) {
				continue;
			}
			output.append(Character.toLowerCase(c));
		}
		return output.toString();
	}
}
