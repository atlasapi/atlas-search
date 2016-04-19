package org.atlasapi.search.loader;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.listing.ProgressStore;
import org.atlasapi.search.searcher.ContentChangeListener;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentBootstrapperTest  {
   
    private final Content item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Content item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Content item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);

    private String taskName;

    @Mock
    private ContentChangeListener listener;

    @Mock
    private ProgressStore progressStore;

    private ContentListingProgress progress;
    private ContentBootstrapper bootstrapper;

    @Before
    public void setUp() throws Exception {
        taskName = "task";
        progress = ContentListingProgress.progressFrom(item1);
        bootstrapper = ContentBootstrapper.builder()
                .withTaskName(taskName)
                .withProgressStore(progressStore)
                .withContentLister(criteria -> {
                    if (ContentListingProgress.START.equals(criteria.getProgress())) {
                        return ImmutableList.of(item1, item2, item3).iterator();
                    } else if(criteria.getProgress().getUri().equals(progress.getUri())) {
                        return ImmutableList.of(item2, item3).iterator();
                    } else {
                        throw new IllegalArgumentException("Unexpected content listing progress");
                    }
                })
                .build();
    }

    @Test
    public void processAllContentInOrder() throws Exception {
        when(progressStore.progressForTask(taskName)).thenReturn(Optional.absent());
        
        bootstrapper.loadAllIntoListener(listener);

        InOrder order = Mockito.inOrder(listener);
        order.verify(listener).beforeContentChange();
        order.verify(listener).contentChange(ImmutableList.of(item1, item2, item3));
        order.verify(listener).afterContentChange();
    }

    @Test
    public void saveProgressAfterProcessing() throws Exception {
        when(progressStore.progressForTask(taskName)).thenReturn(Optional.absent());

        bootstrapper.loadAllIntoListener(listener);

        InOrder order = Mockito.inOrder(listener, progressStore);
        order.verify(listener).contentChange(ImmutableList.of(item1, item2, item3));
        order.verify(progressStore).storeProgress(
                taskName, ContentListingProgress.progressFrom(item3)
        );
    }

    @Test
    public void resumeFromProgress() throws Exception {
        when(progressStore.progressForTask(taskName)).thenReturn(Optional.of(progress));

        bootstrapper.loadAllIntoListener(listener);

        verify(listener).contentChange(ImmutableList.of(item2, item3));
        verify(progressStore).storeProgress(
                taskName, ContentListingProgress.progressFrom(item3)
        );
    }
}
