package org.atlasapi.search;

import javax.servlet.http.HttpServletRequest;

public class Utils {

    private Utils() {
        throw new UnsupportedOperationException("Cannot instantiate Utils class");
    }

    public static String fullRequestURL(HttpServletRequest request) {
        StringBuilder requestURL = new StringBuilder(request.getRequestURL().toString());
        String queryString = request.getQueryString();

        if (queryString == null) {
            return requestURL.toString();
        } else {
            return requestURL.append('?').append(queryString).toString();
        }
    }
}
