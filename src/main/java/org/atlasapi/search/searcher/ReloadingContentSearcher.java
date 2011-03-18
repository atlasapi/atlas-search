package org.atlasapi.search.searcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.model.SearchResults;
import org.atlasapi.search.searcher.LuceneContentSearcher.IndexStats;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;

public class ReloadingContentSearcher extends AbstractService implements ContentSearcher {

    private static final long DELAY = 10;
    private final AtomicReference<LuceneContentSearcher> primary;
    
    private final ScheduledExecutorService executor;
    private final MongoDbBackedContentBootstrapper contentBootstrapper;
    
    public ReloadingContentSearcher(MongoDbBackedContentBootstrapper contentBootstrapper) {
        this(contentBootstrapper, Executors.newSingleThreadScheduledExecutor());
    }
    
    public ReloadingContentSearcher(MongoDbBackedContentBootstrapper contentBootstrapper, ScheduledExecutorService executor) {
        this.contentBootstrapper = contentBootstrapper;
        this.primary = new AtomicReference<LuceneContentSearcher>(new LuceneContentSearcher());
        this.executor = executor;
    }
    
    @Override
    protected void doStart() {
        new Thread() {
            public void run() {
                kickOffBootstrap();
            };
        }.start();
        notifyStarted();
    }
    
    @Override
    protected void doStop() {
        throw new UnsupportedOperationException();
    }
    
    @VisibleForTesting
    protected void kickOffBootstrap() {
        this.contentBootstrapper.loadAllIntoListener(primary.get());
        this.executor.scheduleWithFixedDelay(new LoadAndSwapContentSearcher(), DELAY, DELAY, TimeUnit.MINUTES);
    }

    @Override
    public SearchResults search(SearchQuery query) {
        return primary.get().search(query);
    }
    
    class LoadAndSwapContentSearcher implements Runnable {
        @Override
        public void run() {
            LuceneContentSearcher newSearcher = new LuceneContentSearcher();
            contentBootstrapper.loadAllIntoListener(newSearcher);
            primary.set(newSearcher);
        }
    }
    
    public IndexStats stats() {
        return primary.get().stats();
    }
}
