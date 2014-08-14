package com.webonise.cacheservice.tests;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.webonise.cacheservice.cache.CacheServiceClient;

import drf.common.wrappers.entries.EntriesWrapper;
import drf.common.wrappers.entries.TrackEntry;

@ContextConfiguration({ "classpath:cacheservice-test-context.xml" })
@RunWith(SpringJUnit4ClassRunner.class)
public class RedisCacheServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(RedisCacheServiceTest.class);

    @Autowired
    private CacheServiceClient cacheClient;

    @Test
    public void testEntries() {
        LOG.info("Running Test on Entries list");
        String[] entriesArr = { "ENTRIES_20140731" };
        int count = 0;
        for ( String key : entriesArr ) {
            EntriesWrapper entriesWrapper = new EntriesWrapper();
            entriesWrapper.setKey(key);

            entriesWrapper = (EntriesWrapper) cacheClient.read(entriesWrapper);

            if ( entriesWrapper.getEntries().size() > 0 ) {
                for ( TrackEntry trackEntry : entriesWrapper.getEntries() ) {
                    LOG.info("[{}.] TrackEntry of trackId: {} ", ++count, trackEntry.getTrackId());
                }
            }

        }

    }
}
