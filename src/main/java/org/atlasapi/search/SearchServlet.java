package org.atlasapi.search;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResultsError;
import org.atlasapi.search.view.SearchResultsView;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.text.MoreStrings;
import java.util.Arrays;
import java.util.Collections;
import org.atlasapi.media.entity.Specialization;

public class SearchServlet extends HttpServlet {

    private static final Log log = LogFactory.getLog(SearchServlet.class);
    private static final SelectionBuilder SELECTION_BUILDER = Selection.builder();
    private static final long serialVersionUID = 1L;
    private final SearchResultsView view;
    private final DebuggableContentSearcher searcher;

    public SearchServlet(SearchResultsView view, DebuggableContentSearcher searcher) {
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

        Maybe<Float> titleWeighting = getFloatParameter("titleWeighting", request, response, true);
        if (titleWeighting.isNothing()) {
            return;
        }

        Maybe<Float> broadcastWeighting = getFloatParameter("broadcastWeighting", request, response, true);
        if (broadcastWeighting.isNothing()) {
            return;
        }

        Maybe<Float> catchupWeighting = getFloatParameter("catchupWeighting", request, response, true);
        if (catchupWeighting.isNothing()) {
            return;
        }

        Maybe<Float> priorityChannelWeighting = getFloatParameter("priorityChannelWeighting", request, response, false);
        Maybe<Float> firstBroadcastWeighting = getFloatParameter("firstBroadcastWeighting", request, response, false);

        String publishersCsv = request.getParameter("publishers");
        if (Strings.isNullOrEmpty(publishersCsv)) {
            view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing required (and non-empty) parameter 'publishers'"));
            return;
        }

        ImmutableList<Publisher> publishers = ImmutableList.of();
        try {
            publishers = Publisher.fromCsv(publishersCsv);
        } catch (IllegalArgumentException e) {
            view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, e.getMessage()));
            return;
        } catch (Exception e) {
            view.renderError(request, response, new SearchResultsError(HttpStatusCode.SERVER_ERROR, "Problem processing specified publishers"));
            log.error(e.getMessage(), e);
            return;
        }

        String specializationsCsv = request.getParameter("specializations");
        Iterable<Specialization> specializations = null;
        if (specializationsCsv != null && !specializationsCsv.isEmpty()) {
            specializations = Specialization.fromCsv(specializationsCsv);
        } else {
            specializations = Arrays.asList(Specialization.FILM, Specialization.TV, Specialization.RADIO);
        }

        if (request.getParameter("debug") != null) {
            response.setContentType(MimeType.TEXT_PLAIN.toString());
            ServletOutputStream outputStream = response.getOutputStream();
            outputStream.write(searcher.debug(
                    new SearchQuery(title, SELECTION_BUILDER.build(request), specializations, publishers, titleWeighting.requireValue(), broadcastWeighting.requireValue(), catchupWeighting.requireValue(), priorityChannelWeighting, firstBroadcastWeighting)).getBytes());
        } else {
            view.render(searcher.search(new SearchQuery(title, SELECTION_BUILDER.build(request), specializations, publishers, titleWeighting.requireValue(), broadcastWeighting.requireValue(),
                    catchupWeighting.requireValue(), priorityChannelWeighting, firstBroadcastWeighting)), request, response);
        }

    }

    private Maybe<Float> getFloatParameter(String parameterName, HttpServletRequest request, HttpServletResponse response, boolean required) throws IOException {
        String parameterValue = request.getParameter(parameterName);
        if (!Strings.isNullOrEmpty(parameterValue)) {
            if (MoreStrings.containsOnlyDecimalCharacters(parameterValue)) {
                return Maybe.just(Float.parseFloat(parameterValue));
            } else {
                view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Invalid value of parameter '" + parameterName + "'"));
                return Maybe.nothing();
            }
        } else if (required) {
            view.renderError(request, response, new SearchResultsError(HttpStatusCode.BAD_REQUEST, "Missing required parameter '" + parameterName + "'"));
            return Maybe.nothing();
        } else {
            return Maybe.nothing();
        }
    }
}
