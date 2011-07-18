package org.atlasapi.search.searcher;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentTable;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingHandler;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class DummyContentLister implements ContentLister {
    
    private List<Container> containers;
    private List<Item> items;
    
    public DummyContentLister() {
        this.containers = ImmutableList.of();
        this.items = ImmutableList.of();
    }
    
    public DummyContentLister loadContainerLister(List<Container> respondWith) {
        this.containers = respondWith;
        return this;
    }
    
    public DummyContentLister loadTopLevelItemLister(List<Item> respondWith) {
        this.items = respondWith;
        return this;
    }
    
    @Override
    public boolean listContent(Set<ContentTable> tables, ContentListingCriteria criteria, ContentListingHandler handler) {
        
        int total = containers.size() + items.size();
        int count = 0;
        
        for (ContentTable contentTable : tables) {
            if(contentTable.equals(ContentTable.TOP_LEVEL_CONTAINERS)) {
                    progress(containers, contentTable, count, total);
                    handler.handle(containers, progress(containers, contentTable, ++count, total));
            }
            if(contentTable.equals(ContentTable.TOP_LEVEL_ITEMS)) {
                progress(items, contentTable, count, total);
                handler.handle(items, criteria.getProgress());
            }
        }
        return true;
    }

    private ContentListingProgress progress(Iterable<? extends Content> contents, ContentTable contentTable, int count, int total) {
        return ContentListingProgress.progressFor(Iterables.getLast(contents), contentTable).withCount(count).withTotal(total);
    }
    
}
