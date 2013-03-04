package org.atlasapi.search.searcher;

import org.atlasapi.media.entity.Broadcast;

public interface BroadcastBooster {

    public boolean shouldBoost(Broadcast broadcast);

}