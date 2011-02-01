package org.atlasapi.search.view;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.search.model.SearchResults;
import org.atlasapi.search.model.SearchResultsError;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.media.MimeType;

public class JsonSearchResultsView implements SearchResultsView {

	private final Gson gson = new Gson();
	
	@Override
	public void render(SearchResults results, HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setStatus(HttpStatusCode.OK.code());
		response.setContentType(MimeType.APPLICATION_JSON.toString());
		response.setCharacterEncoding(Charsets.UTF_8.toString());
		response.getOutputStream().write(gson.toJson(results).getBytes(Charsets.UTF_8));
	}

	@Override
	public void renderError(HttpServletRequest request, HttpServletResponse response, SearchResultsError error) throws IOException {
		response.setStatus(error.getCode().code());
		response.setContentType(MimeType.APPLICATION_JSON.toString());
		response.setCharacterEncoding(Charsets.UTF_8.toString());
		JsonObject errorObj = new JsonObject();
		errorObj.add("error", gson.toJsonTree(error));
		response.getOutputStream().write(gson.toJson(errorObj).getBytes(Charsets.UTF_8));
	}
}
