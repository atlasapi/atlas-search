package org.atlasapi.search.searcher;

import org.atlasapi.search.model.SearchResults;

import com.metabroadcast.common.query.Selection;

public interface ContentSearcher {

	SearchResults search(String q, Selection selection);

}
