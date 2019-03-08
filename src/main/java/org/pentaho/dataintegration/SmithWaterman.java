package org.pentaho.dataintegration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class SmithWaterman {
	
	private final Table<String, String, Integer> matrix = HashBasedTable.create();
	private final int startGap = 5;
	private final int continueGap = 1;
	
	public SmithWaterman() {
		String[] alphabet = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
				"t", "u", "v", "w", "x", "y", "z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", ".", ",", " ", "-", "/"};
		for (int i = 0; i < alphabet.length; i ++) {
			for (int j = 0; j < alphabet.length; j++) {
				if (alphabet[i].equals(alphabet[j]))
					matrix.put(alphabet[i], alphabet[j], 5);
				else if ((alphabet[i].equals("d") || alphabet[i].equals("t")) &&
						(alphabet[j].equals("d") || alphabet[j].equals("t")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals("g") || alphabet[i].equals("j")) &&
						(alphabet[j].equals("g") || alphabet[j].equals("j")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals("l") || alphabet[i].equals("r")) &&
						(alphabet[j].equals("l") || alphabet[j].equals("e")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals("m") || alphabet[i].equals("n")) &&
						(alphabet[j].equals("m") || alphabet[j].equals("n")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals("b") || alphabet[i].equals("p") || alphabet[i].equals("v")) &&
						(alphabet[j].equals("b") || alphabet[j].equals("p") || alphabet[j].equals("v")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals("a") || alphabet[i].equals("e") || alphabet[i].equals("i")|| 
						alphabet[i].equals("o") || alphabet[i].equals("u")) && (alphabet[j].equals("a") || 
						alphabet[j].equals("e") || alphabet[j].equals("i") || alphabet[j].equals("o") || alphabet[j].equals("u")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals(".") || alphabet[i].equals(",")) &&
						(alphabet[j].equals(".") || alphabet[j].equals(",")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else if ((alphabet[i].equals("-") || alphabet[i].equals("/")) &&
						(alphabet[j].equals("-") || alphabet[j].equals("/")))
					matrix.put(alphabet[i], alphabet[j], 3);
				else 
					matrix.put(alphabet[i], alphabet[j], -3);
			}
		}
	}
	
	public double calcSimilarity(String s1, String s2) {
		return 0.1;
	}
}
