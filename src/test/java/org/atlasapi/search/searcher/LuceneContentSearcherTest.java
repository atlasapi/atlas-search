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

import static com.metabroadcast.common.query.Selection.ALL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.search.model.SearchResults;

import com.google.common.collect.Lists;

public class LuceneContentSearcherTest extends TestCase {
	
	Brand dragonsDen = brand("/den", "Dragon's den");
	Brand theCityGardener = brand("/garden", "The City Gardener");
	Brand eastenders = brand("/eastenders", "Eastenders");
	Brand politicsEast = brand("/politics", "The Politics Show East");
	Brand meetTheMagoons = brand("/magoons", "Meet the Magoons");
	Brand theJackDeeShow = brand("/dee", "The Jack Dee Show");
	Brand peepShow = brand("/peep-show", "Peep Show");
	Brand euromillionsDraw = brand("/draw", "EuroMillions Draw");
	Brand haveIGotNewsForYou = brand("/news", "Have I Got News For You");
	Brand brasseye = brand("/eye", "Brass Eye");
	Brand science = brand("/science", "The Story of Science: Power, Proof and Passion");
	Brand theApprentice = brand("/apprentice", "The Apprentice");

	Item englishForCats = item("/items/cats", "English for cats");
	Item u2 = item("/items/u2", "U2 Ultraviolet");
	
	Item jamieOliversCookingProgramme = item("/items/oliver/1", "Jamie Oliver's cooking programme", "lots of words that are the same alpha beta");
	Item gordonRamsaysCookingProgramme = item("/items/ramsay/2", "Gordon Ramsay's cooking show", "lots of words that are the same alpha beta");
	
	List<Brand> brands = Arrays.asList(dragonsDen, theCityGardener, eastenders, meetTheMagoons, theJackDeeShow, peepShow, haveIGotNewsForYou, euromillionsDraw, brasseye, science, politicsEast, theApprentice);
	List<Item> items = Arrays.asList(englishForCats, jamieOliversCookingProgramme, gordonRamsaysCookingProgramme);
	List<Item> itemsUpdated = Arrays.asList(u2);
	
	LuceneContentSearcher searcher;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		searcher = new LuceneContentSearcher();
		searcher.brandChanged(brands, ContentListener.ChangeType.BOOTSTRAP);
		searcher.itemChanged(items, ContentListener.ChangeType.BOOTSTRAP);
		searcher.itemChanged(itemsUpdated, null);
	}
	
	public void testFindingBrandsByTitle() throws Exception {
		check(searcher.search("Aprentice", ALL), theApprentice);
		check(searcher.search("den", ALL), dragonsDen, theJackDeeShow);
		check(searcher.search("dragon", ALL), dragonsDen);
		check(searcher.search("dragons", ALL), dragonsDen);
		check(searcher.search("drag den", ALL), dragonsDen);
		check(searcher.search("drag", ALL), dragonsDen, euromillionsDraw);
		check(searcher.search("dragon's den", ALL), dragonsDen);
		check(searcher.search("eastenders", ALL),  eastenders);
		check(searcher.search("easteners", ALL),  eastenders);
		check(searcher.search("eastedners", ALL),  eastenders);
		check(searcher.search("politics east", ALL),  politicsEast);
		check(searcher.search("eas", ALL),  eastenders, politicsEast);
		check(searcher.search("east", ALL),  eastenders, politicsEast);
		check(searcher.search("end", ALL));
		check(searcher.search("peep show", ALL),  peepShow);
		check(searcher.search("peep s", ALL),  peepShow);
		check(searcher.search("dee", ALL),  theJackDeeShow, dragonsDen);
		check(searcher.search("show", ALL),  peepShow, politicsEast, theJackDeeShow);
		check(searcher.search("jack show", ALL),  theJackDeeShow);
		check(searcher.search("the jack dee s", ALL),  theJackDeeShow);
		check(searcher.search("dee show", ALL),  theJackDeeShow);
		check(searcher.search("hav i got news", ALL),  haveIGotNewsForYou);
		check(searcher.search("brasseye", ALL),  brasseye);
		check(searcher.search("braseye", ALL),  brasseye);
		check(searcher.search("brassey", ALL),  brasseye);
		check(searcher.search("The Story of Science Power Proof and Passion", ALL),  science);
		check(searcher.search("The Story of Science: Power, Proof and Passion", ALL),  science);
	}
	
	public void testUsesPrefixSearchForShortSearches() throws Exception {
		check(searcher.search("D", ALL),  dragonsDen);
		check(searcher.search("Dr", ALL),  dragonsDen);
		check(searcher.search("a", ALL));
	}
	
	public void testLimitAndOffset() throws Exception {
		check(searcher.search("eas", ALL),  eastenders, politicsEast);

	}
	
	public void testFindingItemsByTitle() throws Exception {
//		check(searcher.itemTitleSearch("cats", ALL),  englishForCats);
//		check(searcher.itemTitleSearch("u2", ALL),  u2);
	}
	
	public void testUpdateByType() throws Exception {
		Brand dragonsDenV2 = brand("/den", "Dragon's den Version 2");
		
		check(searcher.search("dragon", ALL),  dragonsDen);
		searcher.brandChanged(Lists.newArrayList(dragonsDenV2), ContentListener.ChangeType.CONTENT_UPDATE);
		check(searcher.search("dragon", ALL),  dragonsDen);
	}

	private void check(SearchResults result, Identified... content) {
		assertThat(result.toUris(), is(toUris(Arrays.asList(content))));
	}

	private List<String> toUris(List<? extends Identified> content) {
		List<String> uris = Lists.newArrayList();
		for (Identified description : content) {
			uris.add(description.getCanonicalUri());
		}
		return uris;
	}

	private Brand brand(String uri, String title) {
		Brand b = new Brand(uri, uri, Publisher.BBC);
		b.setTitle(title);
		return b;
	}
	
	private Item item(String uri, String title) {
		return item(uri, title, null);
	}
	
	private Item item(String uri, String title, String description) {
		Item i = new Item();
		i.setTitle(title);
		i.setCanonicalUri(uri);
		i.setDescription(description);
		return i;
	}
}
