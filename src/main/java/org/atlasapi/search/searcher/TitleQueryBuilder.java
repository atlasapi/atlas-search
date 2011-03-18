/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.search.searcher;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Version;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class TitleQueryBuilder {

	private static final int USE_PREFIX_SEARCH_UP_TO = 2;
	
	Query build(String queryString) {	
		
		List<String> tokens = tokens(queryString);
		
		if (shouldUsePrefixSearch(tokens)) {
			return prefixSearch(Iterables.getOnlyElement(tokens));
		} else {
			return fuzzyTermSearch(flatten(queryString), tokens);
		}
	}

	private boolean shouldUsePrefixSearch(List<String> tokens) {
		return tokens.size() == 1 && Iterables.getOnlyElement(tokens).length() <= USE_PREFIX_SEARCH_UP_TO;
	}

	private Query prefixSearch(String token) {
		return new PrefixQuery(new Term(LuceneContentSearcher.FIELD_TITLE_FLATTENED, token));
	}

	private BooleanQuery fuzzyTermSearch(String flattenedQuery, List<String> tokens) {
		BooleanQuery queryForTerms = new BooleanQuery();

		for(String token : tokens) {
			
			BooleanQuery queryForThisTerm = new BooleanQuery();
			
			Term term = new Term(LuceneContentSearcher.FIELD_CONTENT_TITLE, token);
			
			PrefixQuery prefix = new PrefixQuery(term);
			queryForThisTerm.add(prefix, Occur.SHOULD);
			
			queryForThisTerm.add(new FuzzyQuery(term, 0.65f, USE_PREFIX_SEARCH_UP_TO),Occur.SHOULD);
			queryForTerms.add(queryForThisTerm, Occur.MUST);
		}
	
		BooleanQuery either = new BooleanQuery(); 
		either.add(queryForTerms, Occur.SHOULD);
		either.add(fuzzyWithoutSpaces(flattenedQuery), Occur.SHOULD);
		
		Query prefix = prefixSearch(flattenedQuery);
		prefix.setBoost(4);
		either.add(prefix, Occur.SHOULD);
		
		Query exactMatch = new TermQuery(new Term(LuceneContentSearcher.FIELD_TITLE_FLATTENED, flattenedQuery));
		exactMatch.setBoost(100);
		either.add(exactMatch, Occur.SHOULD);
		
		BooleanQuery query = new BooleanQuery();
		query.add(either, Occur.MUST);
		return query;
	}

	private FuzzyQuery fuzzyWithoutSpaces(String flattened) {
		return new FuzzyQuery(new Term(LuceneContentSearcher.FIELD_TITLE_FLATTENED, flattened), 0.8f, USE_PREFIX_SEARCH_UP_TO);
	}
	
	private static List<String> tokens(String queryString) {
		TokenStream tokens = new StandardAnalyzer(Version.LUCENE_30).tokenStream("", new StringReader(queryString));
		List<String> tokensAsStrings = Lists.newArrayList();
		try {
			while(tokens.incrementToken()) {
				TermAttribute token = tokens.getAttribute(TermAttribute.class);
				tokensAsStrings.add(token.term());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return tokensAsStrings;
	}

	public String flatten(String title) {
		return title.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
	}
}
