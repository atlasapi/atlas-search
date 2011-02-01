package org.atlasapi.search;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.search.model.SearchResultsError;
import org.atlasapi.search.searcher.ContentSearcher;
import org.atlasapi.search.view.SearchResultsView;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

public class SearchServlet extends HttpServlet {

	private static final SelectionBuilder SELECTION_BUILDER = Selection.builder();

	private static final long serialVersionUID = 1L;
	
	private final SearchResultsView view;
	private final ContentSearcher searcher;

	public SearchServlet(SearchResultsView view, ContentSearcher searcher) {
		this.view = view;
		this.searcher = searcher;
	}
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String title = request.getParameter("title");
		if (title == null) {
			view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing parameter 'title'"));
			return;
		}
		view.render(searcher.search(title, SELECTION_BUILDER.build(request)), request, response);
	}
}
