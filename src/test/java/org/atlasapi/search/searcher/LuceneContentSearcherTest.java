/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.search.searcher;

import static org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder.broadcast;
import static org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder.complexItem;
import static org.atlasapi.media.entity.testing.VersionTestDataBuilder.version;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder;
import org.atlasapi.persistence.content.DummyKnownTypeContentResolver;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;

public class LuceneContentSearcherTest extends TestCase {

    private static final ImmutableSet<Publisher> ALL_PUBLISHERS = ImmutableSet.copyOf(Publisher.values());
    
    private final Brand dragonsDen = brand("/den", "Dragon's den");
    private final Item dragonsDenItem = complexItem().withBrand(dragonsDen).withVersions(broadcast().buildInVersion()).build();
    private final Brand doctorWho = brand("/doctorwho", "Doctor Who");
    private final Item doctorWhoItem = complexItem().withBrand(doctorWho).withVersions(broadcast().buildInVersion()).build();
    private final Brand theCityGardener = brand("/garden", "The City Gardener");
    private final Item theCityGardenerItem = complexItem().withBrand(theCityGardener).withVersions(broadcast().buildInVersion()).build();
    private final Brand eastenders = brand("/eastenders", "Eastenders");
    private final Item eastendersItem = complexItem().withBrand(eastenders).withVersions(broadcast().buildInVersion()).build();
    private final Brand eastendersWeddings = brand("/eastenders-weddings", "Eastenders Weddings");
    private final Item eastendersWeddingsItem = complexItem().withBrand(eastendersWeddings).withVersions(broadcast().buildInVersion()).build();
    private final Brand politicsEast = brand("/politics", "The Politics Show East");
    private final Item politicsEastItem = complexItem().withBrand(politicsEast).withVersions(broadcast().buildInVersion()).build();
    private final Brand meetTheMagoons = brand("/magoons", "Meet the Magoons");
    private final Item meetTheMagoonsItem = complexItem().withBrand(meetTheMagoons).withVersions(broadcast().buildInVersion()).build();
    private final Brand theJackDeeShow = brand("/dee", "The Jack Dee Show");
    private final Item theJackDeeShowItem = complexItem().withBrand(theJackDeeShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand peepShow = brand("/peep-show", "Peep Show");
    private final Item peepShowItem = complexItem().withBrand(peepShow).withVersions(broadcast().buildInVersion()).build();
    private final Brand euromillionsDraw = brand("/draw", "EuroMillions Draw");
    private final Item euromillionsDrawItem = complexItem().withBrand(euromillionsDraw).withVersions(broadcast().buildInVersion()).build();
    private final Brand haveIGotNewsForYou = brand("/news", "Have I Got News For You");
    private final Item haveIGotNewsForYouItem = complexItem().withBrand(haveIGotNewsForYou).withVersions(broadcast().buildInVersion()).build();
    private final Brand brasseye = brand("/eye", "Brass Eye");
    private final Item brasseyeItem = complexItem().withBrand(brasseye).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand science = brand("/science", "The Story of Science: Power, Proof and Passion");
    private final Item scienceItem = complexItem().withBrand(science).withVersions(ComplexBroadcastTestDataBuilder.broadcast().buildInVersion()).build();
    private final Brand theApprentice = brand("/apprentice", "The Apprentice");
    private final Item theApprenticeItem = complexItem().withBrand(theApprentice).withVersions(broadcast().buildInVersion()).build();
    
    private final Item apparent = complexItem().withTitle("Without Apparent Motive").withUri("/item/apparent").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final Item englishForCats = complexItem().withUri("/items/cats").withTitle("English for cats").withVersions(version().withBroadcasts(broadcast().build()).build()).build();
    private final Item u2 = complexItem().withUri("/items/u2").withTitle("U2 Ultraviolet").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final Item spooks = complexItem().withTitle("Spooks").withUri("/item/spooks")
            .withVersions(version().withBroadcasts(broadcast().withStartTime(new SystemClock().now().minus(Duration.standardDays(365))).build()).build()).build();
    private final Item spookyTheCat = complexItem().withTitle("Spooky the Cat").withUri("/item/spookythecat").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final Item jamieOliversCookingProgramme = complexItem().withUri("/items/oliver/1").withTitle("Jamie Oliver's cooking programme")
            .withDescription("lots of words that are the same alpha beta").withVersions(broadcast().buildInVersion()).build();
    private final Item gordonRamsaysCookingProgramme = complexItem().withUri("/items/ramsay/2").withTitle("Gordon Ramsay's cooking show").withDescription("lots of words that are the same alpha beta")
            .withVersions(broadcast().buildInVersion()).build();
    
    private final Brand rugby = brand("/rugby", "Rugby");
    private final Item rugbyItem = complexItem().withBrand(rugby).withVersions(ComplexBroadcastTestDataBuilder.broadcast().withChannel("http://minor-channel").buildInVersion()).build();
    
    private final Brand sixNationsRugby = brand("/sixnations", "Six Nations Rugby Union");
    private final Item sixNationsRugbyItem = complexItem().withBrand(sixNationsRugby).withVersions(ComplexBroadcastTestDataBuilder.broadcast().withChannel("http://www.bbc.co.uk/services/bbcone/east").buildInVersion()).build();

    private final Brand hellsKitchen = brand("/hellskitchen", "Hell's Kitchen");
    private final Item hellsKitchenItem = complexItem().withBrand(hellsKitchen).withVersions(broadcast().withRepeat(true).buildInVersion()).build();
    
    private final Brand hellsKitchenUSA = brand("/hellskitchenusa", "Hell's Kitchen");
    private final Item hellsKitchenUSAItem = complexItem().withBrand(hellsKitchenUSA).withVersions(broadcast().buildInVersion()).build();
    
    private final Item we = complexItem().withTitle("W.E.").withUri("/item/we").withVersions(version().withBroadcasts(broadcast().build()).build()).build();

    private final List<Brand> brands = Arrays.asList(doctorWho, eastendersWeddings, dragonsDen, theCityGardener, eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou,
            euromillionsDraw, brasseye, science, politicsEast, theApprentice, rugby, sixNationsRugby, hellsKitchen, hellsKitchenUSA);

    private final List<Item> items = Arrays.asList(apparent, englishForCats, jamieOliversCookingProgramme, gordonRamsaysCookingProgramme, spooks, spookyTheCat, dragonsDenItem, doctorWhoItem,
            theCityGardenerItem, eastendersItem, eastendersWeddingsItem, politicsEastItem, meetTheMagoonsItem, theJackDeeShowItem, peepShowItem, euromillionsDrawItem, haveIGotNewsForYouItem,
            brasseyeItem, scienceItem, theApprenticeItem, rugbyItem, sixNationsRugbyItem, hellsKitchenItem, hellsKitchenUSAItem, we);

    private final List<Item> itemsUpdated = Arrays.asList(u2);

    private LuceneContentSearcher searcher;
    private DummyKnownTypeContentResolver contentResolver;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Iterable<Described> allContent = Iterables.<Described> concat(brands, items, itemsUpdated);
        contentResolver = new DummyKnownTypeContentResolver().respondTo(allContent);
        searcher = new LuceneContentSearcher(contentResolver);
        searcher.contentChange(allContent);
    }

    public void testFindingBrandsByTitle() throws Exception {
        check(searcher.search(title("Aprentice")), theApprentice);
        check(searcher.search(currentWeighted("apprent")), theApprentice, apparent);
        check(searcher.search(title("den")), dragonsDen, theJackDeeShow);
        check(searcher.search(title("dragon")), dragonsDen);
        check(searcher.search(title("dragons")), dragonsDen);
        check(searcher.search(title("drag den")), dragonsDen);
        check(searcher.search(title("drag")), dragonsDen, euromillionsDraw);
        check(searcher.search(title("dragon's den")), dragonsDen);
        check(searcher.search(title("eastenders")), eastenders, eastendersWeddings);
        check(searcher.search(title("easteners")), eastenders, eastendersWeddings);
        check(searcher.search(title("eastedners")), eastenders, eastendersWeddings);
        check(searcher.search(title("politics east")), politicsEast);
        check(searcher.search(title("eas")), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(title("east")), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(title("end")));
        check(searcher.search(title("peep show")), peepShow);
        check(searcher.search(title("peep s")), peepShow);
        check(searcher.search(title("dee")), theJackDeeShow, dragonsDen);
        check(searcher.search(title("jack show")), theJackDeeShow);
        check(searcher.search(title("the jack dee s")), theJackDeeShow);
        check(searcher.search(title("dee show")), theJackDeeShow);
        check(searcher.search(title("hav i got news")), haveIGotNewsForYou);
        check(searcher.search(title("brasseye")), brasseye);
        check(searcher.search(title("braseye")), brasseye);
        check(searcher.search(title("brassey")), brasseye);
        check(searcher.search(title("The Story of Science Power Proof and Passion")), science);
        check(searcher.search(title("The Story of Science: Power, Proof and Passion")), science);
        check(searcher.search(title("Jamie")), jamieOliversCookingProgramme);
        check(searcher.search(title("Spooks")), spooks, spookyTheCat);
        check(searcher.search(title("WE")), we);
    }

    protected static SearchQuery title(String term) {
        return new SearchQuery(term, Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.just(100.0f), Maybe.<Float>nothing());
    }

    protected static SearchQuery currentWeighted(String term) {
        return new SearchQuery(term, Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.2f, 0.2f, Maybe.just(100.0f), Maybe.<Float>nothing());
    }

    public void testLimitingToPublishers() throws Exception {
        check(searcher.search(new SearchQuery("east", Selection.ALL, ImmutableSet.of(Publisher.BBC, Publisher.YOUTUBE), 1.0f, 0.0f, 0.0f, Maybe.just(100.0f), Maybe.<Float>nothing())), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(new SearchQuery("east", Selection.ALL, ImmutableSet.of(Publisher.ARCHIVE_ORG, Publisher.YOUTUBE), 1.0f, 0.0f, 0.0f, Maybe.just(100.0f), Maybe.<Float>nothing())));

        Brand east = new Brand("/east", "curie", Publisher.ARCHIVE_ORG);
        east.setTitle("east");
        Item eastItem = complexItem().withVersions(broadcast().buildInVersion()).withBrand(east).build();

        contentResolver.respondTo(ImmutableList.of(east, eastItem));

        searcher.contentChange(ImmutableList.of(east));
        check(searcher.search(new SearchQuery("east", Selection.ALL, ImmutableSet.of(Publisher.ARCHIVE_ORG, Publisher.YOUTUBE), 1.0f, 0.0f, 0.0f, Maybe.just(100.0f), Maybe.<Float>nothing())), east);
    }

    public void testUsesPrefixSearchForShortSearches() throws Exception {
        check(searcher.search(title("D")), doctorWho, dragonsDen);
        check(searcher.search(title("Dr")), doctorWho, dragonsDen);
        check(searcher.search(title("l")));
    }

    public void testLimitAndOffset() throws Exception {
        check(searcher.search(new SearchQuery("eas", Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.<Float>nothing(), Maybe.<Float>nothing())), eastenders, eastendersWeddings, politicsEast);
        check(searcher.search(new SearchQuery("eas", Selection.limitedTo(2), ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.<Float>nothing(), Maybe.<Float>nothing())), eastenders, eastendersWeddings);
        check(searcher.search(new SearchQuery("eas", Selection.offsetBy(2), ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.<Float>nothing(), Maybe.<Float>nothing())), politicsEast);
    }

    public void testBroadcastLocationWeighting() {
        check(searcher.search(currentWeighted("spooks")), spooks, spookyTheCat);

        check(searcher.search(title("spook")), spooks, spookyTheCat);
        check(searcher.search(currentWeighted("spook")), spookyTheCat, spooks);
    }

    public void testPriorityChannelWeighting() {
    	check(searcher.search(new SearchQuery("rugby", Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.just(1.0f), Maybe.<Float>nothing())), rugby, sixNationsRugby);
    	check(searcher.search(new SearchQuery("rugby", Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.<Float>nothing(), Maybe.<Float>nothing())), sixNationsRugby, rugby);
    }
    
    public void testFirstBroadcastWeighting() {
        check(searcher.search(new SearchQuery("hell's kitchen", Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.<Float>nothing(), Maybe.just(1.0f))), hellsKitchen, hellsKitchenUSA);
        check(searcher.search(new SearchQuery("hell's kitchen", Selection.ALL, ALL_PUBLISHERS, 1.0f, 0.0f, 0.0f, Maybe.<Float>nothing(), Maybe.<Float>nothing())), hellsKitchenUSA, hellsKitchen );
    }
    
    protected static void check(SearchResults result, Identified... content) {
        assertThat(result.toUris(), is(toUris(Arrays.asList(content))));
    }

    private static List<String> toUris(List<? extends Identified> content) {
        List<String> uris = Lists.newArrayList();
        for (Identified description : content) {
            uris.add(description.getCanonicalUri());
        }
        return uris;
    }

    protected static Brand brand(String uri, String title) {
        Brand b = new Brand(uri, uri, Publisher.BBC);
        b.setTitle(title);
        return b;
    }

    protected static Item item(String uri, String title) {
        return item(uri, title, null);
    }

    protected static Item item(String uri, String title, String description) {
        Item i = new Item();
        i.setTitle(title);
        i.setCanonicalUri(uri);
        i.setDescription(description);
        i.setPublisher(Publisher.BBC);
        return i;
    }

    protected static Person person(String uri, String title) {
        Person p = new Person(uri, uri, Publisher.BBC);
        p.setTitle(title);
        return p;
    }
    
    protected static ComplexBroadcastTestDataBuilder priorityChannelBroadcast() {
    	return org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder.broadcast().withChannel("http://www.bbc.co.uk/services/bbcone/london");
    }
}
