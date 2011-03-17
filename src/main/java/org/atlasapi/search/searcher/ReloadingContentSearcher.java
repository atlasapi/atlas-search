package org.atlasapi.search.searcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.model.SearchResults;

import com.google.common.util.concurrent.AbstractService;

public class ReloadingContentSearcher extends AbstractService implements ContentSearcher {

    private static final long DELAY = 10;
    private final AtomicReference<LuceneContentSearcher> primary;
    
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final MongoDbBackedContentBootstrapper contentBootstrapper;
    
    public ReloadingContentSearcher(MongoDbBackedContentBootstrapper contentBootstrapper) {
        this.contentBootstrapper = contentBootstrapper;
        this.primary = new AtomicReference<LuceneContentSearcher>(new LuceneContentSearcher());
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
    
    private void kickOffBootstrap() {
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
}
