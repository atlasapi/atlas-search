package org.atlasapi.search.searcher;

import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.RetrospectiveContentLister;

import com.google.common.collect.ImmutableList;

public class DummyContentLister implements RetrospectiveContentLister {
    
    private List<Content> respondWith;

    public DummyContentLister(List<Content> respondWith) {
        this.respondWith = respondWith;
    }
    
    public void loadLister(List<Content> respondWith) {
        this.respondWith = respondWith;
    }

    @Override
    public List<Content> listAllRoots(String arg0, int arg1) {
        List<Content> result = ImmutableList.copyOf(respondWith);
        respondWith = ImmutableList.<Content>of();
        return result;
    }
}
