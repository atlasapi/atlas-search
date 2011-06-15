package org.atlasapi.search.searcher;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentLister;
import org.atlasapi.persistence.content.ContentListingHandler;
import org.atlasapi.persistence.content.ContentListingProgress;
import org.atlasapi.persistence.content.ContentTable;

import com.google.common.collect.ImmutableList;

public class DummyContentLister implements ContentLister {
    
    private List<Container<?>> containers;
    private List<Item> items;
    
    public DummyContentLister() {
        this.containers = ImmutableList.of();
        this.items = ImmutableList.of();
    }
    
    public DummyContentLister loadContainerLister(List<Container<?>> respondWith) {
        this.containers = respondWith;
        return this;
    }
    
    public DummyContentLister loadTopLevelItemLister(List<Item> respondWith) {
        this.items = respondWith;
        return this;
    }
    
    @Override
    public boolean listContent(Set<ContentTable> tables, ContentListingProgress progress, ContentListingHandler handler) {
        
        for (ContentTable contentTable : tables) {
            if(contentTable.equals(ContentTable.TOP_LEVEL_CONTAINERS)) {
                for (Container<?> container : containers) {
                    handler.handle(container, progress);
                }
            }
            if(contentTable.equals(ContentTable.TOP_LEVEL_ITEMS)) {
                for (Item item : items) {
                    handler.handle(item, progress);
                }
            }
        }
        return true;
    }
    
}
