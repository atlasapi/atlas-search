package org.atlasapi.search.searcher;

import org.atlasapi.search.model.SearchResults;

public interface ContentSearcher {

	SearchResults search(SearchQuery query);

}
