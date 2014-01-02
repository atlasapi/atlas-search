package org.atlasapi.search.searcher;

import static org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder.broadcast;
import static org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder.complexItem;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder;
import org.atlasapi.persistence.content.DummyKnownTypeContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.metabroadcast.common.base.Maybe;

@RunWith(MockitoJUnitRunner.class)
public class ReloadingContentBootstrapperTest {

    private final Brand dragonsDen = LuceneContentIndexTest.brand("/den", "Dragon's den");
    private final Item dragonsDenItem = complexItem().withBrand(dragonsDen).withVersions(broadcast().buildInVersion()).build();
    private final Brand theCityGardener = LuceneContentIndexTest.brand("/garden", "The City Gardener");
    private final Item theCityGardenerItem = complexItem().withBrand(theCityGardener).withVersions(broadcast().buildInVersion()).build();
    private final Brand eastenders = LuceneContentIndexTest.brand("/eastenders", "Eastenders");
    private final Item eastendersItem = complexItem().withBrand(eastenders).withVersions(broadcast().buildInVersion()).build();
    private final Brand politicsEast = LuceneContentIndexTest.brand("/politics", "The Politics Show East");
    private final Item politicsEastItem = complexItem().withBrand(politicsEast).withVersions(broadcast().buildInVersion()).build();
    private final Brand meetTheMagoons = LuceneContentIndexTest.brand("/magoons", "Meet the Magoons");
    private final Item meetTheMagoonsItem = complexItem().withBrand(meetTheMagoons).withVersions(broadcast().buildInVersion()).build();
    private final Brand theJackDeeShow = LuceneContentIndexTest.brand("/dee", "The Jack Dee Show");
    private final Item theJackDeeShowItem = complexItem().withBrand(theJackDeeShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand peepShow = LuceneContentIndexTest.brand("/peep-show", "Peep Show");
    private final Item peepShowItem = complexItem().withBrand(peepShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand euromillionsDraw = LuceneContentIndexTest.brand("/draw", "EuroMillions Draw");
    private final Item euromillionsDrawItem = complexItem().withBrand(euromillionsDraw).withVersions(broadcast().buildInVersion()).build();
    private final Brand haveIGotNewsForYou = LuceneContentIndexTest.brand("/news", "Have I Got News For You");
    private final Item haveIGotNewsForYouItem = complexItem().withBrand(haveIGotNewsForYou).withVersions(broadcast().buildInVersion()).build();
    private final Brand brasseye = LuceneContentIndexTest.brand("/eye", "Brass Eye");
    private final Item brasseyeItem = complexItem().withBrand(brasseye).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand science = LuceneContentIndexTest.brand("/science", "The Story of Science: Power, Proof and Passion");
    private final Item scienceItem = complexItem().withBrand(science).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand theApprentice = LuceneContentIndexTest.brand("/apprentice", "The Apprentice");
    private final Item theApprenticeItem = complexItem().withBrand(theApprentice).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Item englishForCats = LuceneContentIndexTest.item("/items/cats", "English for cats");
    private final Item u2 = LuceneContentIndexTest.item("/items/u2", "U2 Ultraviolet");
    private final Item jamieOliversCookingProgramme = LuceneContentIndexTest.item("/items/oliver/1", "Jamie Oliver's cooking programme", "lots of words that are the same alpha beta");
    private final Item gordonRamsaysCookingProgramme = LuceneContentIndexTest.item("/items/ramsay/2", "Gordon Ramsay's cooking show", "lots of words that are the same alpha beta");
    private final List<Container> containers = Arrays.<Container>asList(dragonsDen, theCityGardener, eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou, euromillionsDraw,
            brasseye, science, politicsEast, theApprentice);
    private final List<Item> items = ImmutableList.of(englishForCats, jamieOliversCookingProgramme, gordonRamsaysCookingProgramme, u2, dragonsDenItem, theCityGardenerItem, eastendersItem,
            politicsEastItem, meetTheMagoonsItem, theJackDeeShowItem, peepShowItem, euromillionsDrawItem, haveIGotNewsForYouItem, brasseyeItem, scienceItem, theApprenticeItem);
    private final DummyContentLister retroLister = new DummyContentLister().loadContainerLister(containers).loadTopLevelItemLister(items);
    private final ContentBootstrapper bootstrapper = new ContentBootstrapper().withContentListers(retroLister);
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final KnownTypeContentResolver contentResolver = new DummyKnownTypeContentResolver().respondTo(containers).respondTo(items);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private volatile LuceneContentIndex searcher;
    private volatile ReloadingContentBootstrapper reloader;

    @Before
    public void setUp() throws Exception {
        File luceneDir = Files.createTempDir();
        luceneDir.deleteOnExit();
        searcher = new LuceneContentIndex(luceneDir, contentResolver, new DummyBroadcastBooster(), channelResolver);
        reloader = new ReloadingContentBootstrapper(searcher, bootstrapper, scheduler, true, 180, TimeUnit.MINUTES);
    }

    @Test
    public void shouldLoadAndReloadSearch() {
        when(channelResolver.fromUri(anyString())).thenReturn(Maybe.<Channel>nothing());  
        bootstrapper.loadAllIntoListener(searcher);
        reloader.kickOffBootstrap();
        testSearcher();
        retroLister.loadContainerLister(containers);
        retroLister.loadTopLevelItemLister(items);

        scheduler.tick(15, TimeUnit.MINUTES);

        DateTime stopTesting = new DateTime().plusSeconds(2);
        while (new DateTime().isBefore(stopTesting)) {
            testSearcher();
        }
    }

    private void testSearcher() {
        LuceneContentIndexTest.check(searcher.search(LuceneContentIndexTest.title("Aprentice")), theApprentice);
    }
    
    // TODO: Add tests for this
    private static class DummyBroadcastBooster implements BroadcastBooster {

        @Override
        public boolean shouldBoost(Broadcast broadcast) {
            return false;
        }
        
    }
}
