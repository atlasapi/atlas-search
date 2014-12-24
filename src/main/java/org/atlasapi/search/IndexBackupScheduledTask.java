package org.atlasapi.search;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.search.searcher.LuceneContentIndex;

import com.google.common.base.Throwables;
import com.metabroadcast.common.scheduling.ScheduledTask;


public class IndexBackupScheduledTask extends ScheduledTask {
    
    private LuceneContentIndex index;
    
    public IndexBackupScheduledTask(LuceneContentIndex index) {
        this.index = checkNotNull(index);
    }

    @Override
    protected void runTask() {
        try {
            index.backup();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
    
}
