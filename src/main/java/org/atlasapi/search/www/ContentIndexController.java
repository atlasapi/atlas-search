package org.atlasapi.search.www;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.search.searcher.LuceneContentIndex;
import org.springframework.stereotype.Controller;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


@Controller
public class ContentIndexController extends HttpServlet {

    private static final String URI_PARAMETER = "uri";
    
    private final ContentResolver contentResolver;
    private final LuceneContentIndex index;
    
    public ContentIndexController(ContentResolver contentResolver, LuceneContentIndex index) {
        this.contentResolver = checkNotNull(contentResolver);
        this.index = checkNotNull(index);
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        index.beforeContentChange();
        index.contentChange(Iterables.filter(
                contentResolver.findByCanonicalUris(ImmutableSet.of(request.getParameter(URI_PARAMETER))).getAllResolvedResults(), Described.class));
        index.afterContentChange();
        response.getWriter().write("DONE");
    }
}
