package org.atlasapi.search.searcher;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.units.ByteCount;

public class LuceneSearcherProbe implements HealthProbe {

    private static final ByteCount MAX_INDEX_SIZE = ByteCount.gibibytes(1);
    private static final Duration MAX_INDEX_STALENESS = Duration.standardHours(12);
    private final Clock clock;
    private final ReloadingContentBootstrapper index;
    private final String slug;

    public LuceneSearcherProbe(String slug, ReloadingContentBootstrapper index) {
        this.slug = slug;
        this.index = index;
        this.clock = new SystemClock();
    }

    @Override
    public ProbeResult probe() {
        ProbeResult result = new ProbeResult(title());
        DateTime lastIndexBuild = index.lastIndexBuild();
        result.add("last index rebuild finish time", lastIndexBuild.toString("dd/MM/yy HH:mm"), lastIndexBuild != null && clock.now().minus(MAX_INDEX_STALENESS).isBefore(lastIndexBuild));
        return result;
    }

    @Override
    public String title() {
        return "Lucene index";
    }

    @Override
    public String slug() {
        return slug;
    }
}
