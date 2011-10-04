package org.atlasapi.search.searcher;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class DummyContentLister implements ContentLister {
    
    private List<Content> containers;
    private List<Content> items;
    
    public DummyContentLister() {
        this.containers = ImmutableList.of();
        this.items = ImmutableList.of();
    }
    
    public DummyContentLister loadContainerLister(List<Container> respondWith) {
        this.containers = ImmutableList.<Content>copyOf(respondWith);
        return this;
    }
    
    public DummyContentLister loadTopLevelItemLister(List<Item> respondWith) {
        this.items = ImmutableList.<Content>copyOf(respondWith);
        return this;
    }
    
    @Override
    public Iterator<Content> listContent(ContentListingCriteria criteria) {
        
        ImmutableList.Builder<Iterator<Content>> iterators = ImmutableList.builder();
        
        for (ContentCategory contentTable : criteria.getCategories()) {
            if(contentTable.equals(ContentCategory.CONTAINER)) {
                iterators.add(containers.iterator());
            }
            if(contentTable.equals(ContentCategory.TOP_LEVEL_ITEM)) {
                iterators.add(items.iterator());
            }
        }
        
        return Iterators.concat(iterators.build().iterator());
    }
    
}
