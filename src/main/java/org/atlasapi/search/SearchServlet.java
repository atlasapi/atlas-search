package org.atlasapi.search;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResultsError;
import org.atlasapi.search.view.SearchResultsView;

import com.google.common.base.Strings;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.text.MoreStrings;

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
			view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing required parameter 'title'"));
			return;
		}
		
		float titleWeighting = 1.0f;
		String titleWeightingParam = request.getParameter("titleWeighting");
		if (!Strings.isNullOrEmpty(titleWeightingParam)) {
		    if (MoreStrings.containsOnlyDecimalCharacters(titleWeightingParam)) {
		        titleWeighting = Float.parseFloat(titleWeightingParam);
		    } else {
		        view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Invalid value of parameter 'titleWeighting'"));
		        return;
		    }
		} else {
		    view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing required parameter 'titleWeighting'"));
		    return;
		}
		
		float currentnessWeighting = 0.0f;
		String currentnessWeightingParam = request.getParameter("currentnessWeighting");
        if (!Strings.isNullOrEmpty(currentnessWeightingParam)) {
            if (MoreStrings.containsOnlyDecimalCharacters(currentnessWeightingParam)) {
                currentnessWeighting = Float.parseFloat(currentnessWeightingParam);
            } else {
                view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Invalid value of parameter 'currentnessWeighting'"));
                return;
            }
        } else {
            view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing required parameter 'currentnessWeighting'"));
            return;
        }
		
		String publishersCsv = request.getParameter("publishers");
		if (Strings.isNullOrEmpty(publishersCsv)) {
			view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing required (and non-empty) parameter 'publishers'"));
			return;
		}
		
		view.render(searcher.search(new SearchQuery(title, SELECTION_BUILDER.build(request), Publisher.fromCsv(publishersCsv), titleWeighting, currentnessWeighting)), request, response);
	}
}
