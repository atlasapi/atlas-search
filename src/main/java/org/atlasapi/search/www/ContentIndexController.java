package org.atlasapi.search.www;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.MongoProgressStore;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.search.AtlasSearchModule;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentIndex;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;


@Controller
public class ContentIndexController extends HttpServlet {

    private static final String URI_PARAMETER = "uri";
    private static final String PUBLISHER_PARAMETER = "publisher";

    @Autowired
    private MongoProgressStore progressStore;
    @Autowired
    private DatabasedMongo mongo;
    @Autowired
    private MongoContentResolver mongoContentResolver;

    private final ContentResolver contentResolver;
    private final LuceneContentIndex index;

    public ContentIndexController(ContentResolver contentResolver, LuceneContentIndex index) {
        this.contentResolver = checkNotNull(contentResolver);
        this.index = checkNotNull(index);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String uri = request.getParameter(URI_PARAMETER);
        String publisher = request.getParameter(PUBLISHER_PARAMETER);

        if(uri!=null) {
            index.beforeContentChange();
            index.contentChange(Iterables.filter(
                    contentResolver.findByCanonicalUris(ImmutableSet.of(uri))
                            .getAllResolvedResults(), Described.class));
            index.afterContentChange();
        }
        if(publisher!=null){
            AtlasSearchModule atlasSearchModule = new AtlasSearchModule();
            mongo = atlasSearchModule.mongo();
            progressStore = atlasSearchModule.progressStore();
            mongoContentResolver = atlasSearchModule.contentResolver();

            ContentListingCriteria.Builder criteriaBuilder = defaultCriteria()
                    .forPublishers(ImmutableSet.<Publisher>builder()
                            .add(Publisher.valueOf(publisher))
                            .build()
                            .asList()
                    );

            ContentBootstrapper.BuildStep bootstrapperBuilder = ContentBootstrapper.builder()
                    .withTaskName("owl-search-bootstrap-mongo-api-request")
                    .withProgressStore(progressStore)
                    .withContentLister(new MongoContentLister(this.mongo, mongoContentResolver))
                    .withCriteriaBuilder(criteriaBuilder);

            ContentBootstrapper build = bootstrapperBuilder.build();
            build.loadAllIntoListener(index);
        }
        response.getWriter().write("DONE");
    }
}
