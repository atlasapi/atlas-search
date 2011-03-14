package org.atlasapi.search.loader;


import static org.hamcrest.Matchers.hasItems;

import java.util.List;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.MongoTestHelper;

@RunWith(JMock.class)
public class MongoDbBackedContentListenerTest  {
   
	private final Mockery context = new Mockery();
	
    private ContentListener listener = context.mock(ContentListener.class);
    private MongoDbBackedContentStore store = new MongoDbBackedContentStore(MongoTestHelper.anEmptyTestDatabase());
   
    private MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(listener, store);
    
    
    
    private final Item item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Item item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Item item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);
    
    @Test
    public void testShouldAllContents() throws Exception {
        
        store.createOrUpdate(item1);
        store.createOrUpdate(item2);
        store.createOrUpdate(item3);
        bootstrapper.setBatchSize(2);
        
        context.checking(new Expectations() {{
            one(listener).itemChanged(with(hasItems(item1, item2)), with(ContentListener.ChangeType.BOOTSTRAP));
            one(listener).itemChanged(with(hasItems(item3)), with(ContentListener.ChangeType.BOOTSTRAP));
        }});
        
        bootstrapper.loadAll();
    }
}
