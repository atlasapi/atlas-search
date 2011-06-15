package org.atlasapi.search.loader;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentLister;
import org.atlasapi.persistence.content.ContentListingHandler;
import org.atlasapi.persistence.content.ContentListingProgress;
import org.atlasapi.persistence.content.ContentTable;
import org.atlasapi.search.searcher.ContentChangeListener;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;

@RunWith(JMock.class)
public class MongoDbBackedContentListenerTest  {
   
	private final Mockery context = new Mockery();
    
    private final Item item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Item item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Item item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);
	
    private ContentChangeListener listener = context.mock(ContentChangeListener.class);
    private final ContentLister lister = new ContentLister() {

        List<Item> contents = ImmutableList.of(item1, item2, item3);

        @Override
        public boolean listContent(Set<ContentTable> tables, ContentListingProgress progress, ContentListingHandler handler) {
            for (ContentTable contentTable : tables) {
                if(contentTable.equals(ContentTable.TOP_LEVEL_ITEMS)) {
                    for (Item item : contents) {
                        handler.handle(item, progress);
                    }
                }
            }
            return true;
        }
    };
   
    private MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(lister);
    
    @Test
    public void testShouldAllContents() throws Exception {
        
        bootstrapper.setBatchSize(2);
        
        context.checking(new Expectations() {{
            one(listener).contentChange(item1);
            one(listener).contentChange(item2);
            one(listener).contentChange(item3);
        }});
        
        bootstrapper.loadAllIntoListener(listener);
    }
}
