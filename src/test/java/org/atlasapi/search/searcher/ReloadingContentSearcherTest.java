package org.atlasapi.search.searcher;

import static org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder.broadcast;
import static org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder.complexItem;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder;
import org.atlasapi.persistence.content.DummyKnownTypeContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.UUID;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.junit.Before;

@RunWith(JMock.class)
public class ReloadingContentSearcherTest {

    private final Brand dragonsDen = LuceneContentSearcherTest.brand("/den", "Dragon's den");
    private final Item dragonsDenItem = complexItem().withBrand(dragonsDen).withVersions(broadcast().buildInVersion()).build();
    private final Brand theCityGardener = LuceneContentSearcherTest.brand("/garden", "The City Gardener");
    private final Item theCityGardenerItem = complexItem().withBrand(theCityGardener).withVersions(broadcast().buildInVersion()).build();
    private final Brand eastenders = LuceneContentSearcherTest.brand("/eastenders", "Eastenders");
    private final Item eastendersItem = complexItem().withBrand(eastenders).withVersions(broadcast().buildInVersion()).build();
    private final Brand politicsEast = LuceneContentSearcherTest.brand("/politics", "The Politics Show East");
    private final Item politicsEastItem = complexItem().withBrand(politicsEast).withVersions(broadcast().buildInVersion()).build();
    private final Brand meetTheMagoons = LuceneContentSearcherTest.brand("/magoons", "Meet the Magoons");
    private final Item meetTheMagoonsItem = complexItem().withBrand(meetTheMagoons).withVersions(broadcast().buildInVersion()).build();
    private final Brand theJackDeeShow = LuceneContentSearcherTest.brand("/dee", "The Jack Dee Show");
    private final Item theJackDeeShowItem = complexItem().withBrand(theJackDeeShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand peepShow = LuceneContentSearcherTest.brand("/peep-show", "Peep Show");
    private final Item peepShowItem = complexItem().withBrand(peepShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand euromillionsDraw = LuceneContentSearcherTest.brand("/draw", "EuroMillions Draw");
    private final Item euromillionsDrawItem = complexItem().withBrand(euromillionsDraw).withVersions(broadcast().buildInVersion()).build();
    private final Brand haveIGotNewsForYou = LuceneContentSearcherTest.brand("/news", "Have I Got News For You");
    private final Item haveIGotNewsForYouItem = complexItem().withBrand(haveIGotNewsForYou).withVersions(broadcast().buildInVersion()).build();
    private final Brand brasseye = LuceneContentSearcherTest.brand("/eye", "Brass Eye");
    private final Item brasseyeItem = complexItem().withBrand(brasseye).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand science = LuceneContentSearcherTest.brand("/science", "The Story of Science: Power, Proof and Passion");
    private final Item scienceItem = complexItem().withBrand(science).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand theApprentice = LuceneContentSearcherTest.brand("/apprentice", "The Apprentice");
    private final Item theApprenticeItem = complexItem().withBrand(theApprentice).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Item englishForCats = LuceneContentSearcherTest.item("/items/cats", "English for cats");
    private final Item u2 = LuceneContentSearcherTest.item("/items/u2", "U2 Ultraviolet");
    private final Item jamieOliversCookingProgramme = LuceneContentSearcherTest.item("/items/oliver/1", "Jamie Oliver's cooking programme", "lots of words that are the same alpha beta");
    private final Item gordonRamsaysCookingProgramme = LuceneContentSearcherTest.item("/items/ramsay/2", "Gordon Ramsay's cooking show", "lots of words that are the same alpha beta");
    private final List<Container> containers = Arrays.<Container>asList(dragonsDen, theCityGardener, eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou, euromillionsDraw,
            brasseye, science, politicsEast, theApprentice);
    private final List<Item> items = ImmutableList.of(englishForCats, jamieOliversCookingProgramme, gordonRamsaysCookingProgramme, u2, dragonsDenItem, theCityGardenerItem, eastendersItem,
            politicsEastItem, meetTheMagoonsItem, theJackDeeShowItem, peepShowItem, euromillionsDrawItem, haveIGotNewsForYouItem, brasseyeItem, scienceItem, theApprenticeItem);
    private final DummyContentLister retroLister = new DummyContentLister().loadContainerLister(containers).loadTopLevelItemLister(items);
    private final ContentBootstrapper bootstrapper = new ContentBootstrapper().withContentListers(retroLister);
    @SuppressWarnings("unused")
    private final Mockery context = new Mockery();
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final KnownTypeContentResolver contentResolver = new DummyKnownTypeContentResolver().respondTo(containers).respondTo(items);
    private volatile ReloadingContentSearcher reloader;

    @Before
    public void setUp() throws Exception {
        File luceneDir = new File(System.getProperty("java.io.tmpdir") + File.separatorChar + UUID.randomUUID());
        if (luceneDir.mkdir()) {
            luceneDir.deleteOnExit();
            LuceneContentSearcher searcher = new LuceneContentSearcher(luceneDir, contentResolver);
            reloader = new ReloadingContentSearcher(searcher, bootstrapper, scheduler);
        } else {
            throw new IllegalStateException();
        }
    }

    @Test
    public void shouldLoadAndReloadSearch() {
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
        LuceneContentSearcherTest.check(reloader.search(LuceneContentSearcherTest.title("Aprentice")), theApprentice);
    }
}
