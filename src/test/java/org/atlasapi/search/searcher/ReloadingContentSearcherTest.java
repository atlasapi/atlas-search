package org.atlasapi.search.searcher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;

@RunWith(JMock.class)
public class ReloadingContentSearcherTest {
    
    private final Brand dragonsDen = LuceneContentSearcherTest.brand("/den", "Dragon's den");
    private final Brand theCityGardener = LuceneContentSearcherTest.brand("/garden", "The City Gardener");
    private final Brand eastenders = LuceneContentSearcherTest.brand("/eastenders", "Eastenders");
    private final Brand politicsEast = LuceneContentSearcherTest.brand("/politics", "The Politics Show East");
    private final Brand meetTheMagoons = LuceneContentSearcherTest.brand("/magoons", "Meet the Magoons");
    private final Brand theJackDeeShow = LuceneContentSearcherTest.brand("/dee", "The Jack Dee Show");
    private final Brand peepShow = LuceneContentSearcherTest.brand("/peep-show", "Peep Show");
    private final Brand euromillionsDraw = LuceneContentSearcherTest.brand("/draw", "EuroMillions Draw");
    private final Brand haveIGotNewsForYou = LuceneContentSearcherTest.brand("/news", "Have I Got News For You");
    private final Brand brasseye = LuceneContentSearcherTest.brand("/eye", "Brass Eye");
    private final Brand science = LuceneContentSearcherTest.brand("/science", "The Story of Science: Power, Proof and Passion");
    private final Brand theApprentice = LuceneContentSearcherTest.brand("/apprentice", "The Apprentice");

    private final Item englishForCats = LuceneContentSearcherTest.item("/items/cats", "English for cats");
    private final Item u2 = LuceneContentSearcherTest.item("/items/u2", "U2 Ultraviolet");
    
    private final Item jamieOliversCookingProgramme = LuceneContentSearcherTest.item("/items/oliver/1", "Jamie Oliver's cooking programme", "lots of words that are the same alpha beta");
    private final Item gordonRamsaysCookingProgramme = LuceneContentSearcherTest.item("/items/ramsay/2", "Gordon Ramsay's cooking show", "lots of words that are the same alpha beta");
    
    private final List<Container<?>> containers = Arrays.<Container<?>>asList(dragonsDen, theCityGardener, eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou, euromillionsDraw, brasseye, science, politicsEast, theApprentice);
    private final List<Item> items = ImmutableList.of(englishForCats, jamieOliversCookingProgramme, gordonRamsaysCookingProgramme, u2);
    
    private final DummyContentLister retroLister = new DummyContentLister().loadContainerLister(containers).loadTopLevelItemLister(items);
    
    private final MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(retroLister);
    @SuppressWarnings("unused")
    private final Mockery context = new Mockery();
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    
    private final ReloadingContentSearcher reloader = new ReloadingContentSearcher(bootstrapper, scheduler);
    
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
