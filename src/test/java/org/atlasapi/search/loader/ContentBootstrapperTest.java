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
public class ContentBootstrapperTest  {
   
	private final Mockery context = new Mockery();

    private final Item item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Item item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Item item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);
	
    private ContentChangeListener listener = context.mock(ContentChangeListener.class);
    
    private final ContentLister lister1 = new ContentLister() {

        List<Content> contents = ImmutableList.<Content>of(item1, item2);

        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            return contents.iterator();
        }
    };
    private final ContentLister lister2 = new ContentLister() {

        List<Content> contents = ImmutableList.<Content>of(item3);

        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            return contents.iterator();
        }
    };
   
    private ContentBootstrapper bootstrapper = new ContentBootstrapper().withContentListers(lister1, lister2);
    
    @Test
    public void testShouldAllContents() throws Exception {
        
        context.checking(new Expectations() {{
            one(listener).beforeContentChange();
            one(listener).contentChange(ImmutableList.of(item1, item2));
            one(listener).contentChange(ImmutableList.of(item3));
            one(listener).afterContentChange();
        }});
        
        bootstrapper.loadAllIntoListener(listener);
    }
}
