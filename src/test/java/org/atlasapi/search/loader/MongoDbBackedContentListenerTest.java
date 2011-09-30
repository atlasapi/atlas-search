package org.atlasapi.search.loader;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
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

        List<Content> contents = ImmutableList.<Content>of(item1, item2, item3);

        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            return contents.iterator();
        }
    };
   
    private MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(lister);
    
    @Test
    public void testShouldAllContents() throws Exception {
        
        context.checking(new Expectations() {{
            one(listener).contentChange(ImmutableList.of(item1, item2, item3));
        }});
        
        bootstrapper.loadAllIntoListener(listener);
    }
}
