package edu.ucla.cs;

import java.io.*;

public class DataProcessing {
	
	File file;
	
	public DataProcessing(String fileName) {
		file = new File(fileName);
		try {
			FileReader reader = new FileReader(file);
			BufferedReader breader = new BufferedReader(reader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	public void read() {
		
	}
}
