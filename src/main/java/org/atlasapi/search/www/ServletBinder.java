package org.atlasapi.search.www;

import javax.servlet.http.HttpServlet;

public interface ServletBinder {
	
	ServletBinder bind(String path, HttpServlet servlet);
	
}
