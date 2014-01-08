package org.atlasapi.search.searcher;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Throwables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.health.ProbeResult.ProbeResultEntry;
import com.metabroadcast.common.health.ProbeResult.ProbeResultType;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class LuceneSearcherProbe implements HealthProbe {

    private static final String LAST_INDEX_BUILD_KEY = "last index rebuild finish time";
    private static final DateTimeFormatter dateFormat= ISODateTimeFormat.dateTime();
    private static final ProbeResultEntry NO_LAST_BUILD
        = new ProbeResultEntry(ProbeResultType.INFO, LAST_INDEX_BUILD_KEY, "nil");

    private final Clock clock;
    private final ReloadingContentBootstrapper index;
    private final String slug;
    private final Duration maxStaleness;

    public LuceneSearcherProbe(String slug, ReloadingContentBootstrapper index, Duration maxStaleness) {
        this.slug = slug;
        this.index = index;
        this.maxStaleness = maxStaleness;
        this.clock = new SystemClock();
    }

    @Override
    public ProbeResult probe() {
        ProbeResult result = new ProbeResult(title());
        try {
            DateTime lastIndexBuild = index.lastIndexBuild();
            result.addEntry(lastIndexBuildEntry(lastIndexBuild));
        } catch (Exception e) {
            result.add("Probe exception", Throwables.getStackTraceAsString(e), false);
        }
        return result;
    }

    private ProbeResultEntry lastIndexBuildEntry(DateTime lastIndexBuild) {
        if (lastIndexBuild == null) {
            return NO_LAST_BUILD;
        }
        ProbeResultType type = indexBuiltAndFresh(lastIndexBuild) ? ProbeResultType.SUCCESS
                                                                  : ProbeResultType.FAILURE;
        return new ProbeResultEntry(type, LAST_INDEX_BUILD_KEY, dateFormat.print(lastIndexBuild));
    }

    private boolean indexBuiltAndFresh(DateTime lastIndexBuild) {
        DateTime oldestAcceptableIndexBuild = clock.now().minus(maxStaleness);
        return lastIndexBuild != null
                && lastIndexBuild.isAfter(oldestAcceptableIndexBuild);
    }

    @Override
    public String title() {
        return "Lucene index: " + slug;
    }

    @Override
    public String slug() {
        return slug;
    }
}
