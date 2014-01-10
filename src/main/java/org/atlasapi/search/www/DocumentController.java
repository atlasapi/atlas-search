package org.atlasapi.search.www;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.search.searcher.LuceneContentIndex;
import org.springframework.stereotype.Controller;

@Controller
public class DocumentController extends HttpServlet {

    private static final String URI_PARAMETER = "uri";
    private final LuceneContentIndex index;
    
    public DocumentController(LuceneContentIndex index) {
        this.index = checkNotNull(index);
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String parameter = request.getParameter(URI_PARAMETER);
        response.getWriter().append(index.document(parameter).get());
    }
}
