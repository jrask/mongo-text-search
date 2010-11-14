package com.jayway.mongodb;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import com.mongodb.BasicDBObject;

public class AnalyzedDBObject extends BasicDBObject {

	private static final long serialVersionUID = -3375115283230844689L;

	public static enum Condition {ALL,IN}
	
	private Analyzer analyzer;

	public AnalyzedDBObject(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public AnalyzedDBObject createQuery(String name,String text) throws IOException {
		return createQuery(name, text, Condition.ALL);
	}
	
	public AnalyzedDBObject createQuery(String name,String text,Condition condition) throws IOException {
		List<String> tokens = tokenize(analyzer.tokenStream(name, new StringReader(text)));
		append(name,new BasicDBObject(
				String.format("$%s",condition.toString().toLowerCase()),
				tokens.toArray(new String[0])));
		return this;
	}
	
	public AnalyzedDBObject indexFullText(String name, String text)
			throws IOException {
		append(name,
				tokenize(analyzer.tokenStream(name, new StringReader(text))));
		return this;
	}

	private List<String> tokenize(TokenStream stream) throws IOException {
		List<String> tokens = new ArrayList<String>();
		TermAttribute term = (TermAttribute) stream
				.addAttribute(TermAttribute.class);
		while (stream.incrementToken()) {
			// Not sure if we somehow can use termBuffer() to get a char[]
			// so we do no have to create a new String for each term
			tokens.add(term.term());
		}
		return tokens;
	}

}
