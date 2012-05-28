package org.atlasapi.search.searcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import org.atlasapi.search.loader.ContentBootstrapper;

public class ReloadingContentBootstrapper extends AbstractService {

    private volatile ContentChangeListener listener;
    private final ScheduledExecutorService executor;
    private final ContentBootstrapper contentBootstrapper;
    private final long delayInMillis;
    private final Log log = LogFactory.getLog(ReloadingContentBootstrapper.class);

    public ReloadingContentBootstrapper(LuceneContentIndex listener, ContentBootstrapper contentBootstrapper, long delay, TimeUnit unit) {
        this(listener, contentBootstrapper, Executors.newSingleThreadScheduledExecutor(), delay, unit);
    }

    protected ReloadingContentBootstrapper(LuceneContentIndex listener, ContentBootstrapper contentBootstrapper, ScheduledExecutorService executor, long delay, TimeUnit unit) {
        this.contentBootstrapper = contentBootstrapper;
        this.listener = listener;
        this.executor = executor;
        this.delayInMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
    }

    @Override
    protected void doStart() {
        new Thread() {

            public void run() {
                kickOffBootstrap();
            }
        ;
        }.start();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    protected void kickOffBootstrap() {
        try {
            this.contentBootstrapper.loadAllIntoListener(listener);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        this.executor.scheduleWithFixedDelay(new LoadContentSearcher(), delayInMillis, delayInMillis, TimeUnit.MILLISECONDS);
    }

    class LoadContentSearcher implements Runnable {

        @Override
        public void run() {
            try {
                log.info("Loading content searcher");
                contentBootstrapper.loadAllIntoListener(listener);
                log.info("Finished loading content searcher");
            } catch (Exception e) {
                log.error(e);
            }
        }
    }
}
