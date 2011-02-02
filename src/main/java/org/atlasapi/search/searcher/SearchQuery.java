package org.atlasapi.search.searcher;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.query.Selection;

public class SearchQuery {

	private final String term;
	private final Selection selection;
	private final Set<Publisher> includedPublishers;

	public SearchQuery(String term, Selection selection, Iterable<Publisher> includedPublishers) {
		this.term = term;
		this.selection = selection;
		this.includedPublishers = ImmutableSet.copyOf(includedPublishers);
	}
	
	public String getTerm() {
		return term;
	}
	
	public Selection getSelection() {
		return selection;
	}
	
	public Set<Publisher> getIncludedPublishers() {
		return includedPublishers;
	}
}
