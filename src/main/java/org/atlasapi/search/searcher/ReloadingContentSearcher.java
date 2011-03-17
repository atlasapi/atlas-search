package org.atlasapi.search.searcher;

import java.util.concurrent.atomic.AtomicReference;

import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.model.SearchResults;

public class ReloadingContentSearcher implements ContentSearcher {

    private final AtomicReference<LuceneContentSearcher> primary;
    private final AtomicReference<LuceneContentSearcher> secondry;
    private final RetrospectiveContentLister retroListener;
    
    public ReloadingContentSearcher(RetrospectiveContentLister retroListener) {
        this.retroListener = retroListener;
        this.primary = new AtomicReference<LuceneContentSearcher>(new LuceneContentSearcher());
        this.secondry = new AtomicReference<LuceneContentSearcher>(new LuceneContentSearcher());
        
        new MongoDbBackedContentBootstrapper(primary.get(), retroListener).start();
    }

    @Override
    public SearchResults search(SearchQuery query) {
        return primary.get().search(query);
    }
}
