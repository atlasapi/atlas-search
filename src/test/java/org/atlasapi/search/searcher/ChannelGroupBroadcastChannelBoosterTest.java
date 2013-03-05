package org.atlasapi.search.searcher;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.math.BigInteger;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.entity.Broadcast;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

@RunWith(MockitoJUnitRunner.class)
public class ChannelGroupBroadcastChannelBoosterTest {

    private static final String BOOST_CHANNEL_URI = "http://channel.example.com";

    private static final long CHANNEL_GROUP_ID = 12;
    private static final long CHANNEL_ID = 19;
    
    @Mock
    private final ChannelGroupResolver channelGroupResolver = mock(ChannelGroupResolver.class);
    
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    
    private final Channel channel = Channel.builder()
                                        .withUri(BOOST_CHANNEL_URI)
                                        .build();
    
    private final ChannelGroup channelGroup = new Region();

    private final ChannelNumbering channelNumbering = ChannelNumbering.builder()
                                        .withChannel(CHANNEL_ID)
                                        .withChannelNumber("101")
                                        .withChannelGroup(CHANNEL_GROUP_ID)
                                        .build();
    
    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec();
    
    private ChannelGroupBroadcastChannelBooster booster;
    
    @Before
    public void setUp() {
        channel.setId(CHANNEL_ID);
        channelGroup.addChannelNumbering(channelNumbering);
        when(channelGroupResolver.channelGroupFor(CHANNEL_GROUP_ID)).thenReturn(Optional.of(channelGroup));
        when(channelResolver.fromId(CHANNEL_ID)).thenReturn(Maybe.just(channel));
        
        booster = new ChannelGroupBroadcastChannelBooster(channelGroupResolver, channelResolver, codec.encode(BigInteger.valueOf(CHANNEL_GROUP_ID)));
    }
    
    @Test
    public void testBoostsBroadcastsOnPriorityChannel() {
        Broadcast boostingBroadcast = new Broadcast(BOOST_CHANNEL_URI, new DateTime().plusMinutes(30), new DateTime().plusHours(1));
        assertTrue(booster.shouldBoost(boostingBroadcast));
        
        Broadcast nonBoostingBroadcast = new Broadcast("http://anotherchannel.example.com/", new DateTime().plusMinutes(30), new DateTime().plusHours(1));
        assertFalse(booster.shouldBoost(nonBoostingBroadcast));
    }
    
    @Test
    public void testDoesntBoostOldBroadcastsOnPriorityChannel() {
        Broadcast boostingBroadcast = new Broadcast(BOOST_CHANNEL_URI, new DateTime().minusDays(30), new DateTime().minusDays(29));
        assertFalse(booster.shouldBoost(boostingBroadcast));
    }
}
