package org.atlasapi.search.view;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.search.model.SearchResults;
import org.atlasapi.search.model.SearchResultsError;

public interface SearchResultsView {

	void render(SearchResults results, HttpServletRequest request, HttpServletResponse response) throws IOException;

	void renderError(HttpServletRequest request, HttpServletResponse response, SearchResultsError error) throws IOException;

}
