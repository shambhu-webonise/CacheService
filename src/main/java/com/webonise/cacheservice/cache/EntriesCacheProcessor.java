package com.webonise.cacheservice.cache;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.drf.common.dto.changes.ChangesDTO;
import com.drf.common.dto.common.DayDTO;
import com.drf.common.dto.common.RaceKeyDTO;
import com.drf.common.dto.entry.CardDTO;
import com.drf.common.dto.entry.EntryDTO;
import com.drf.common.dto.entry.HorseDTO;
import com.drf.common.dto.entry.RaceDTO;
import com.drf.common.dto.entry.TrackDTO;

import drf.common.wrappers.entries.AvailableDates;
import drf.common.wrappers.entries.ChangesTrackDTO;
import drf.common.wrappers.entries.EntriesWrapper;
import drf.common.wrappers.entries.HorseDTOWrapper;
import drf.common.wrappers.entries.RaceDTOWrapper;
import drf.common.wrappers.entries.RaceDetailWrapper;
import drf.common.wrappers.entries.RacesWrapper;
import drf.common.wrappers.entries.TrackEntry;
import drf.common.wrappers.util.Utility;

@Component
public class EntriesCacheProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(EntriesCacheProcessor.class);

    @Autowired
    private DataHelper dataHelper;

    @Autowired
    private CacheServiceClient cacheClient;

    public void buildCache() {
        try {
            List<TrackDTO> tracks = dataHelper.getEntries();
            if ( tracks != null ) {
                updateTracksEntriesInMemcached(tracks, Utility.PAST_30_DAY_LIMIT, Utility.FUTURE_7_DAY_LIMIT);
                writeAllRacesOfTrackInMemcached(tracks);
                writeRaceOnTrackInMemcached(tracks);
                updateEntriesTrackAvailableDates(tracks);
            }
        } catch (Exception e) {
            LOG.error("Error is deserilizing Entries DTO", e);
        }
    }

    private void updateTracksEntriesInMemcached(List<TrackDTO> tracks, int pastDayLimit, int futureDayLimit) {
        try {
            if ( tracks != null ) {
                updateAvaibleRaceDates(tracks);
                for ( TrackDTO trackDTO : tracks ) {
                    List<CardDTO> cards = trackDTO.getCards();
                    for ( CardDTO cardDTO : cards ) {
                        RaceKeyDTO raceKey = cardDTO.getRaceKey();
                        DayDTO raceDate = raceKey.getRaceDate();
                        Date date = raceDate.getDate();

                        // If updated file date is out of range
                        if ( !Utility.isValidDate(date, pastDayLimit, futureDayLimit) ) {
                            continue;
                        }

                        com.drf.common.dto.changes.TrackDTO changesTrackDTO = this.writeChangesKeyInMemcached(raceKey).getChanges();
                        List<Integer> changesList = getChangesList(changesTrackDTO);

                        String entriesKey = Utility.getEntriesKey(date);
                        EntriesWrapper entriesWrapper = new EntriesWrapper();
                        entriesWrapper.setKey(entriesKey);

                        entriesWrapper = (EntriesWrapper) cacheClient.read(entriesWrapper);

                        if ( entriesWrapper != null ) {
                            Map<String, TrackEntry> entryMap = getAvailableTracksInMemcached(entriesWrapper.getEntries());
                            if ( entryMap.containsKey(raceKey.getTrackId()) ) {
                                TrackEntry trackEntry = (TrackEntry) entryMap.get(raceKey.getTrackId());
                                updateTrackEntryDTOData(trackEntry, trackDTO, cardDTO, raceDate, changesList);
                            } else {
                                TrackEntry trackEntry = this.buildEntriesDTOData(trackDTO, cardDTO, raceDate, changesList);
                                entriesWrapper.getEntries().add(trackEntry);
                            }
                        } else {
                            // build track entry and save into memcached.
                            TrackEntry trackEntry = this.buildEntriesDTOData(trackDTO, cardDTO, raceDate, changesList);
                            entriesWrapper = new EntriesWrapper();
                            entriesWrapper.getEntries().add(trackEntry);
                        }

                        // Cached into Redis
                        cacheClient.save(entriesWrapper);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("Error occured while building Entries", ex);
        }

    }

    private void writeAllRacesOfTrackInMemcached(List<TrackDTO> tracks) {
        try {
            if ( tracks != null ) {
//                Map<String, Object> racesMap = new HashMap<>();
                List<RacesWrapper> racesWrapperList = new ArrayList<RacesWrapper>();
                Map<String, List<String>> trackDateMap = this.buildTracksDateMap(tracks);

                for ( TrackDTO trackDTO : tracks ) {
                    List<CardDTO> cards = trackDTO.getCards();
                    for ( CardDTO cardDTO : cards ) {
                        RaceKeyDTO raceKeyDTO = cardDTO.getRaceKey();
                        DayDTO raceDate = raceKeyDTO.raceDate;
                        Date date = raceDate.getDate();
                        String entriesKey = Utility.getRacesKey(date, raceKeyDTO.getTrackId(), raceKeyDTO.getCountry());
                        List<String> trackDateList = trackDateMap.get(raceKeyDTO.getTrackId());
                        try {
                            RacesWrapper trackRacesWrapper = this.buildRacesOnTracksDTOData(trackDTO, cardDTO, raceDate, trackDateList);
                            trackRacesWrapper.setKey(entriesKey);
                            racesWrapperList.add(trackRacesWrapper);
                            // racesMap.put(entriesKey, trackRacesWrapper);
                        } catch (Exception ex) {
                            LOG.error("Entery DTO not found for track: {}, date:{}", cardDTO.getRaceKey().getTrackId(), raceDate.getDate(), ex);
                        }
                    }
                }
                this.buildMemcache(racesWrapperList);
            }
        } catch (Exception ex) {
            LOG.error("Error is deserilizing Entries DTO", ex);
        }

    }

    private void writeRaceOnTrackInMemcached(List<TrackDTO> tracks) {
        try {
            List<RaceDetailWrapper> raceDetailWrapperList = new ArrayList<RaceDetailWrapper>();

            for ( TrackDTO trackDTO : tracks ) {
                List<CardDTO> cards = trackDTO.getCards();

                for ( CardDTO cardDTO : cards ) {
                    RaceKeyDTO raceKeyDTO = cardDTO.getRaceKey();
                    DayDTO raceDate = raceKeyDTO.raceDate;
                    Date date = raceDate.getDate();

                    EntryDTO entry;
                    try {
                        entry = dataHelper.getEntryDTO(cardDTO.getRaceKey());
                        
                        if ( entry == null ) {
                            continue;
                        }
                        List<RaceDTO> races = entry.getRaces();

                        for ( RaceDTO raceDTO : races ) {
                            String entriesKey = Utility.getRaceDetailsKey(raceDTO.getRaceKey().getRaceNumber(), raceKeyDTO.getTrackId(), raceKeyDTO.getCountry(), date, raceDTO.getRaceKey().getDayEvening());
                            RaceDetailWrapper raceDetailWrapper = this.buildRaceOnTracksDtoData(trackDTO, cardDTO, raceDTO, raceDate);
                            raceDetailWrapper.setKey(entriesKey);
                            if ( !(isRaceDetailKeyPresentInMemcache(entriesKey)) ) {
                                raceDetailWrapperList.add(raceDetailWrapper);
//                                raceDetailMap.put(entriesKey, raceDetailWrapper);
                            }
                        }
                    } catch (Exception ex) {
                        LOG.error("Unable to get races for track {} date {}", cardDTO.getRaceKey().getTrackId(), cardDTO.getRaceKey().getRaceDate().getDate(), ex);
                    }
                }
            }
            this.buildRace(raceDetailWrapperList);
        } catch (Exception ex) {
            LOG.error("Error is deserilizing Entries DTO", ex);
        }
    }

    private void updateEntriesTrackAvailableDates(List<TrackDTO> tracks) {

    }

    private void updateAvaibleRaceDates(List<TrackDTO> tracks) {
        List<String> raceDateList = new ArrayList<>();
        AvailableDates availableDates = new AvailableDates();
        for ( TrackDTO trackDTO : tracks ) {
            List<CardDTO> cards = trackDTO.getCards();

            for ( CardDTO cardDTO : cards ) {
                DayDTO raceDate = cardDTO.getRaceKey().getRaceDate();
                Date date = raceDate.getDate();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Utility.STR_DATE_FORMAT_yyyyMMdd);
                String dateKey = simpleDateFormat.format(date);
                if ( !raceDateList.contains(dateKey) ) {
                    raceDateList.add(dateKey);
                }
            }
        }
        availableDates.setKey(Utility.AVAILABLE_RACE_DATES);
        cacheClient.read(availableDates);
        availableDates = (AvailableDates) cacheClient.read(availableDates);
        List<String> dateList = null;
        dateList = availableDates.getTrackCalendar();
        if ( dateList != null ) {
            for ( String dateString : raceDateList ) {
                if ( !dateList.contains(dateString) ) {
                    dateList.add(dateString);
                }
            }
        } else {
            availableDates.setTrackCalendar(raceDateList);
        }
        // Caching into Redis
        cacheClient.save(availableDates);
    }

    private ChangesTrackDTO writeChangesKeyInMemcached(RaceKeyDTO raceKey) {

        ChangesTrackDTO changesDTO = new ChangesTrackDTO();

        try {
            changesDTO = (ChangesTrackDTO) dataHelper.getChangesTrackDTO(raceKey);
            if ( changesDTO == null ) {
                LOG.info("Changes DTO file not available for track {} for date {}", raceKey.getTrackId(), raceKey.getRaceDate().toDate());
                return changesDTO;
            }

            String changesKey = Utility.getChangesKey(raceKey.getRaceDate().toDate(), raceKey.getTrackId(), raceKey.getCountry());
            changesDTO.setKey(changesKey);

            cacheClient.save(changesDTO);
        } catch (Exception ex) {
            LOG.error("Error is deserilizing Entries DTO", ex);
        }
        return changesDTO;
    }

    private List<Integer> getChangesList(com.drf.common.dto.changes.TrackDTO changesTrackDTO) {
        List<Integer> raceNumbers = new ArrayList<>();
        if ( changesTrackDTO != null ) {
            List<com.drf.common.dto.changes.RaceDTO> races = changesTrackDTO.getRaces();
            if ( races != null ) {
                for ( com.drf.common.dto.changes.RaceDTO race : races ) {
                    List<ChangesDTO> changes = race.getChanges();
                    for ( ChangesDTO changesDTO : changes ) {
                        if ( !changesDTO.getType().equals(Utility.EMPTY_STRING) ) {
                            raceNumbers.add(race.getRaceNumber());
                            break;
                        }
                    }
                }
            }
        }
        Collections.sort(raceNumbers);
        return raceNumbers;
    }

    private Map<String, TrackEntry> getAvailableTracksInMemcached(List<TrackEntry> entries) {
        Map<String, TrackEntry> entryMap = new HashMap<String, TrackEntry>();
        for ( TrackEntry trackEntry : entries ) {
            entryMap.put(trackEntry.getTrackId(), trackEntry);
        }
        return entryMap;
    }

    private void updateTrackEntryDTOData(TrackEntry trackEntry, TrackDTO trackDTO, CardDTO cardDTO, DayDTO raceDate, List<Integer> changesList) {
        RaceKeyDTO raceKeyDTO = cardDTO.getRaceKey();

        trackEntry.setTrackId(raceKeyDTO.trackId);
        trackEntry.setCountry(raceKeyDTO.country);
        trackEntry.setTrackName(trackDTO.getName());
        trackEntry.setChanges(changesList);

        if ( raceKeyDTO.raceDate.equals(raceDate) ) {
            try {
                EntryDTO entry = dataHelper.getEntryDTO(raceKeyDTO);
                if ( entry != null ) {
                    List<RaceDTO> races = entry.getRaces();
                    if ( races.size() > 0 ) {
                        trackEntry.setSurfaceDescription(races.get(0).getSurfaceDescription());
                    }
                    trackEntry.setWeather(entry.getWeather());
                }
            } catch (Exception ex) {
                LOG.error("Error in fetching Entries DTO for track {} and date {}", raceKeyDTO.getTrackId(), raceKeyDTO.getRaceDate().getDate());
            }
        }
    }

    private TrackEntry buildEntriesDTOData(TrackDTO trackDTO, CardDTO cardDTO, DayDTO raceDate, List<Integer> changesList) throws Exception {
        RaceKeyDTO raceKeyDTO = cardDTO.getRaceKey();

        TrackEntry trackEntry = new TrackEntry();
        trackEntry.setTrackId(raceKeyDTO.trackId);
        trackEntry.setCountry(raceKeyDTO.country);
        trackEntry.setTrackName(trackDTO.getName());
        trackEntry.setChanges(changesList);

        if ( raceKeyDTO.raceDate.equals(raceDate) ) {
            try {
                EntryDTO entry = dataHelper.getEntryDTO(raceKeyDTO);
                if ( entry != null ) {
                    List<RaceDTO> races = entry.getRaces();
                    if ( races.size() > 0 ) {
                        trackEntry.setSurfaceDescription(races.get(0).getSurfaceDescription());
                    }
                    trackEntry.setWeather(entry.getWeather());
                }
            } catch (Exception ex) {
                LOG.error("Error in fetching Entries DTO for track {} and date {}", raceKeyDTO.getTrackId(), raceKeyDTO.getRaceDate().getDate());
            }
        }
        return trackEntry;
    }

    private Map<String, List<String>> buildTracksDateMap(List<TrackDTO> tracks) {
        Map<String, List<String>> trackMap = new HashMap<>();

        for ( TrackDTO trackDTO : tracks ) {
            List<CardDTO> cards = trackDTO.getCards();

            for ( CardDTO cardDTO : cards ) {
                DayDTO raceDate = cardDTO.getRaceKey().getRaceDate();
                Date date = raceDate.getDate();
                if ( !Utility.isValidDate(date, Utility.PAST_30_DAY_LIMIT, Utility.FUTURE_7_DAY_LIMIT) ) {
                    continue;
                }
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Utility.STR_DATE_FORMAT_yyyyMMdd);
                String dateKey = simpleDateFormat.format(date);
                String entriesKey = cardDTO.getRaceKey().getTrackId();
                List<String> dateList;
                if ( trackMap.containsKey(entriesKey) ) {
                    dateList = (ArrayList<String>) trackMap.get(entriesKey);
                } else {
                    dateList = new ArrayList<>();
                    trackMap.put(entriesKey, dateList);
                }
                if ( !dateList.contains(dateKey) ) {
                    dateList.add(dateKey);
                }
            }
        }
        return trackMap;
    }

    private RacesWrapper buildRacesOnTracksDTOData(TrackDTO trackDTO, CardDTO cardDTO, DayDTO raceDate, List<String> trackDateList) throws Exception {
        RacesWrapper racesWrapper = new RacesWrapper();
        RaceKeyDTO raceKeyDTO = cardDTO.getRaceKey();
        racesWrapper.setTrackId(raceKeyDTO.trackId);
        racesWrapper.setCountry(raceKeyDTO.country);
        racesWrapper.setTrackName(trackDTO.getName());
        // racesWrapper.setAvailableRaceDates(trackDateList);

        if ( cardDTO.getRaceKey().raceDate.equals(raceDate) ) {
            try {
                EntryDTO entry = dataHelper.getEntryDTO(raceKeyDTO);
                if ( entry != null ) {
                    racesWrapper.setWeather(entry.getWeather());
                    List<RaceDTO> raceList = entry.getRaces();
                    List<RaceDTOWrapper> races = new ArrayList<>(raceList.size());
                    for ( RaceDTO raceDTO : raceList ) {
                        raceDTO.setHorses(null);
                        RaceDTOWrapper raceDTOWrapper = new RaceDTOWrapper(raceDTO);
                        races.add(raceDTOWrapper);
                    }
                    racesWrapper.setRaces(races);
                }
            } catch (Exception ex) {
                LOG.error("Error in fetching Entries DTO for track {} and date {}", raceKeyDTO.getTrackId(), raceKeyDTO.getRaceDate().getDate(), ex);
            }
        }
        return racesWrapper;
    }

    private void buildMemcache(List<RacesWrapper> racesWrappers) {
        try {
            for ( RacesWrapper racesWrapper : racesWrappers ) {
                cacheClient.save(racesWrapper);
            }
        } catch (Exception ex) {
            LOG.error("Fail to write data in CachedServer ", ex);
        }
    }
    
    private RaceDetailWrapper buildRaceOnTracksDtoData(TrackDTO trackDTO, CardDTO cardDTO, RaceDTO raceDTO, DayDTO raceDate) {
        RaceDetailWrapper raceDetailWrapper = new RaceDetailWrapper();
        raceDetailWrapper.setTrackId(cardDTO.getRaceKey().trackId);
        raceDetailWrapper.setCountry(cardDTO.getRaceKey().country);
        raceDetailWrapper.setTrackName(trackDTO.getName());
        RaceDTOWrapper raceDTOWrapper = new RaceDTOWrapper(raceDTO);
        List<HorseDTOWrapper> horseDTOWrappersList = new ArrayList<>(raceDTO.getHorses().size());
        for ( HorseDTO horseDTO : raceDTO.getHorses() ) {
            horseDTOWrappersList.add(new HorseDTOWrapper(horseDTO));
        }
        raceDTOWrapper.setHorseDTOs(horseDTOWrappersList);
        raceDTOWrapper.setHorses(null);
        raceDetailWrapper.setRace(raceDTOWrapper);
        return raceDetailWrapper;
    }
    
    private boolean isRaceDetailKeyPresentInMemcache(String raceKey) {
        RaceDetailWrapper raceDetailWrapper = new RaceDetailWrapper();
        raceDetailWrapper.setKey(raceKey);
        boolean isRaceDetailKey = false;
        try {
            raceDetailWrapper = (RaceDetailWrapper) cacheClient.read(raceDetailWrapper);
            if ( raceDetailWrapper.getRace() != null ) {
                isRaceDetailKey = true;
            }
        } catch (Exception ex) {
            LOG.error("Unable to fetch race detail key from CachedServer", ex);
        }
        return isRaceDetailKey;
    }
    
    private void buildRace(List<RaceDetailWrapper> raceDetailWrapperList) {
        try {
            for ( RaceDetailWrapper raceWrapper : raceDetailWrapperList ) {
                cacheClient.save(raceWrapper);
            }
        } catch (Exception ex) {
            LOG.error("Fail to write data in CachedServer ", ex);
        }
        
    }
}
