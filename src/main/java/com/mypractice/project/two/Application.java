package com.mypractice.project.two;

public class Application {
	public static void main(String[] args) {
		//InferCsvSchema inferCsvSchema = new InferCsvSchema();
		//inferCsvSchema.printInferCsvSchema();
		
//		DefineCsvSchema defineCsvSchema = new DefineCsvSchema();
//		defineCsvSchema.printDefineSchema();
		
		JSONLineParser jsonLineParser = new JSONLineParser();
		jsonLineParser.parseJsonLine();
	}
}
