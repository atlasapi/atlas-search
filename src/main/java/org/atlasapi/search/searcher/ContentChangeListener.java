package org.atlasapi.search.searcher;

import org.atlasapi.media.entity.Described;


public interface ContentChangeListener {

    void contentChange(Iterable<? extends Described> content);
    
}
