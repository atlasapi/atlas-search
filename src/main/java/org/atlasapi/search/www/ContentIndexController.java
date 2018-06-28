package org.atlasapi.search.www;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.listing.MongoProgressStore;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.search.AtlasSearchModule;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentIndex;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

@Controller
public class ContentIndexController extends HttpServlet {

    private static final String URI_PARAMETER = "uri";
    private static final String PUBLISHER_PARAMETER = "publisher";

    private static final Logger log = LoggerFactory.getLogger(ContentIndexController.class);

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
        // workaround until the autowiring is fixed
        AtlasSearchModule atlasSearchModule = new AtlasSearchModule();
        mongo = atlasSearchModule.mongo();
        progressStore = atlasSearchModule.progressStore();
        mongoContentResolver = atlasSearchModule.contentResolver();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String uri = request.getParameter(URI_PARAMETER);
        String publisher = request.getParameter(PUBLISHER_PARAMETER);

        if (uri != null) {
            log.info("Request to index uri {}", uri);
            index.beforeContentChange();
            Iterable<Described> describeds = Iterables.filter(
                    contentResolver.findByCanonicalUris(ImmutableSet.of(uri))
                            .getAllResolvedResults(), Described.class);
            for (Described content : describeds) {
                index.contentChange(content);
            }
            index.afterContentChange();
            log.info("done");
            response.getWriter().write("DONE");
        }
        if (publisher != null){
            String taskName = "owl-search-bootstrap-mongo-api-request-" + publisher;
            log.info("Request to re-index publisher {}", publisher);
            //start from scratch
            progressStore.storeProgress(taskName, ContentListingProgress.START);

            ContentListingCriteria.Builder criteriaBuilder = defaultCriteria()
                    .forPublishers(ImmutableSet.<Publisher>builder()
                            .add(Publisher.valueOf(publisher))
                            .build()
                            .asList()
                    );

            ContentBootstrapper.BuildStep bootstrapperBuilder = ContentBootstrapper.builder()
                    .withTaskName(taskName)
                    .withProgressStore(progressStore)
                    .withContentLister(new MongoContentLister(mongo, mongoContentResolver))
                    .withCriteriaBuilder(criteriaBuilder);

            ContentBootstrapper build = bootstrapperBuilder.build();

            response.setStatus(HttpStatus.SC_ACCEPTED);
            response.getWriter().write("Request to reindex " + publisher + " was accepted. "
                                       + "All content will be re-indexed.<br>"
                                       + "You can view progress from the listerProgress collection"
                                       + "in mongoDb. _id:\"" + taskName + "\"<br>"
                                       + "You can view a better progress my monitoring the log from"
                                       + "inside atlas-search at "
                                       + "less /usr/local/jetties/atlas-search/work/atlas-search.log");
            response.getWriter().flush();

            build.loadAllIntoListener(index);
            log.info("Publisher {} has been fully re-indexed", publisher);
        }
    }
}
