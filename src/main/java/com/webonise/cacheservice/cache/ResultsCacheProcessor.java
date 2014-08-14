package com.webonise.cacheservice.cache;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.drf.common.dto.common.DayDTO;
import com.drf.common.dto.common.RaceKeyDTO;
import com.drf.common.dto.entry.EntryDTO;
import com.drf.common.dto.entry.HorseDTO;
import com.drf.common.dto.entry.RaceDTO;
import com.drf.common.dto.results.PayoffDTO;
import com.drf.common.dto.results.ResultsDTO;
import com.drf.common.dto.results.RunnerDTO;
import com.drf.common.dto.results.TrackDTO;

import drf.common.wrappers.entries.AvailableDates;
import drf.common.wrappers.results.PayOffDTOWrapper;
import drf.common.wrappers.results.ResultDetailsWrapper;
import drf.common.wrappers.results.ResultEntry;
import drf.common.wrappers.results.ResultRaceDTOWrapper;
import drf.common.wrappers.results.ResultsWrapper;
import drf.common.wrappers.results.RunnerDTOWrapper;
import drf.common.wrappers.util.Utility;

@Component
public class ResultsCacheProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ResultsCacheProcessor.class);

    @Autowired
    private DataHelper dataHelper;

    @Autowired
    private CacheServiceClient cacheClient;

    public void buildCache() {
        try {
            List<TrackDTO> resultsTracks = dataHelper.getResults();
            this.writeResultsEntriesInMemcached(resultsTracks);
            this.writeAllResultsOfTrackInMemcached(resultsTracks);
        } catch (Exception ex) {
            LOG.error("Error is deserilizing Result DTO", ex);
        }
    }

    private void writeResultsEntriesInMemcached(List<TrackDTO> resultsTracks) {
        Map<String, Object> resultEntriesMap = new HashMap<>();

        this.writeAvaibleResultDates(resultsTracks);

        for ( com.drf.common.dto.results.TrackDTO track : resultsTracks ) {
            for ( RaceKeyDTO raceKeyDTO : track.getCards() ) {
                Date date = raceKeyDTO.getRaceDate().toDate();
                String resultsKey = Utility.getResultsEntriesKey(date);
                ResultsWrapper resultsWrapper;
                if ( resultEntriesMap.containsKey(resultsKey) ) {
                    resultsWrapper = (ResultsWrapper) resultEntriesMap.get(resultsKey);
                } else {
                    resultsWrapper = new ResultsWrapper();
                    resultEntriesMap.put(resultsKey, resultsWrapper);
                }
                ResultEntry buildResultsEntryDTOData = this.buildResultsEntryDTOData(track);
                resultsWrapper.getResults().add(buildResultsEntryDTOData);
            }
        }

        this.updateCurrentDayResult(resultEntriesMap);
        this.buildMemcache(resultEntriesMap);
    }

    private void writeAllResultsOfTrackInMemcached(List<TrackDTO> resultsTracks) {
        Map<String, Object> mapData = new HashMap<>();
        Map<String, List<String>> trackDateMap = this.buildTracksResultDateMap(resultsTracks);
        writeAvailableResultDatesTrackList(trackDateMap);
        for ( com.drf.common.dto.results.TrackDTO track : resultsTracks ) {
            List<ResultsDTO> results = new ArrayList<>();
            for ( RaceKeyDTO raceKeyDTO : track.getCards() ) {
                Date date = raceKeyDTO.getRaceDate().toDate();
                String resultsKey = Utility.getResultsDetailsKey(date, raceKeyDTO.getTrackId(), raceKeyDTO.getCountry());
                try {
                    results.add(dataHelper.getResultsDTO(raceKeyDTO));
                } catch (Exception ex) {
                    LOG.error("Error is deserilizing Result DTO for track {} date {}", raceKeyDTO.getTrackId(), raceKeyDTO.getRaceDate().getDate(), ex);
                }
                if ( !(this.isResultDetailKeyPresentInMemcache(resultsKey)) ) {
                    List<String> trackDateList = trackDateMap.get(raceKeyDTO.getTrackId());
                    ResultDetailsWrapper resultsDetailWrapper = this.buildResultsDTOData(track, raceKeyDTO, trackDateList);
                    if ( resultsDetailWrapper != null ) {
                        mapData.put(resultsKey, resultsDetailWrapper);
                    }
                }
            }
        }
        this.buildResultDetails(mapData);

    }

    private void buildResultDetails(Map<String, Object> resultData) {
        try {
            for ( Iterator<String> iterator = resultData.keySet().iterator(); iterator.hasNext(); ) {
                String entreisDateKey = iterator.next();
                ResultDetailsWrapper resultDetailsWrapper = (ResultDetailsWrapper) resultData.get(entreisDateKey);
                resultDetailsWrapper.setKey(entreisDateKey);
                cacheClient.save(resultDetailsWrapper);
            }
        } catch (Exception ex) {
            LOG.error("Failed to cached data in server", ex);
        }
    }

    private void writeAvaibleResultDates(List<com.drf.common.dto.results.TrackDTO> tracks) {
        List<String> raceDateList = new ArrayList<>();
        AvailableDates availableDates = new AvailableDates();
        availableDates.setKey(Utility.AVAILABLE_RESULT_DATES);

        Date currentDate = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Utility.STR_DATE_FORMAT_yyyyMMdd);
        for ( com.drf.common.dto.results.TrackDTO trackDTO : tracks ) {
            List<RaceKeyDTO> raceKeys = trackDTO.getCards();

            for ( RaceKeyDTO raceKeyDTO : raceKeys ) {
                DayDTO raceDate = raceKeyDTO.getRaceDate();
                Date date = raceDate.getDate();

                if ( Utility.isFutureDate(date, currentDate) ) {
                    continue;
                }

                String dateKey = simpleDateFormat.format(date);
                if ( !raceDateList.contains(dateKey) ) {
                    raceDateList.add(dateKey);
                }
            }
        }
        availableDates.setTrackCalendar(raceDateList);
        try {
            cacheClient.save(availableDates);
        } catch (Exception ex) {
            LOG.error("Fail to write AVAILABLE_RESULT_DATES data in cached server", ex);
        }
    }

    private ResultEntry buildResultsEntryDTOData(com.drf.common.dto.results.TrackDTO track) {
        ResultEntry resultEntry = new ResultEntry();
        resultEntry.setCountry(track.getCountry());
        resultEntry.setTrackId(track.getId());
        resultEntry.setTrackName(track.getName());
        return resultEntry;
    }

    private void updateCurrentDayResult(Map<String, Object> resultEntriesMap) {
        String currentResultsKey = Utility.getResultsEntriesKey(new Date());
        ResultsWrapper newResultsWrapper = (ResultsWrapper) resultEntriesMap.get(currentResultsKey);

        if ( newResultsWrapper != null ) {
            ResultsWrapper resultsWrapper = new ResultsWrapper();
            resultsWrapper.setKey(currentResultsKey);
            resultsWrapper = (ResultsWrapper) cacheClient.read(resultsWrapper);
            if ( resultsWrapper != null ) {
                Map<String, ResultEntry> oldResultEntriesDetailMap = this.getResultEntriesDetail(resultsWrapper);
                for ( ResultEntry resultEntry : newResultsWrapper.getResults() ) {
                    if ( !oldResultEntriesDetailMap.containsKey(resultEntry.getTrackId()) ) {
                        resultsWrapper.getResults().add(resultEntry);
                    }
                }
                resultEntriesMap.put(currentResultsKey, resultsWrapper);
            }
        }
    }

    private void buildMemcache(Map<String, Object> resultData) {
        try {
            for ( Iterator<String> iterator = resultData.keySet().iterator(); iterator.hasNext(); ) {
                String entreisDateKey = iterator.next();
                ResultsWrapper resultsWrapper = (ResultsWrapper) resultData.get(entreisDateKey);
                resultsWrapper.setKey(entreisDateKey);
                cacheClient.save(resultsWrapper);
            }
        } catch (Exception ex) {
            LOG.error("Failed to cached data in server", ex);
        }
    }

    private Map<String, ResultEntry> getResultEntriesDetail(ResultsWrapper newResultsWrapper) {
        Map<String, ResultEntry> resultDetailMap = new HashMap<>();
        for ( ResultEntry resultEntry : newResultsWrapper.getResults() ) {
            resultDetailMap.put(resultEntry.getTrackId(), resultEntry);
        }
        return resultDetailMap;
    }

    private Map<String, List<String>> buildTracksResultDateMap(List<com.drf.common.dto.results.TrackDTO> tracks) {
        Map<String, List<String>> trackMap = new HashMap<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Utility.STR_DATE_FORMAT_yyyyMMdd);
        for ( com.drf.common.dto.results.TrackDTO track : tracks ) {
            for ( RaceKeyDTO raceKeyDTO : track.getCards() ) {
                Date date = raceKeyDTO.getRaceDate().toDate();

                String dateKey = simpleDateFormat.format(date);
                String entriesKey = raceKeyDTO.getTrackId();
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

    private void writeAvailableResultDatesTrackList(Map<String, List<String>> trackDateMap) {
        AvailableDates availableDates = new AvailableDates();
        availableDates.setKey(Utility.AVAILABLE_RESULT_DATES);
        availableDates.setAvailableDates(trackDateMap);
        cacheClient.save(availableDates);
    }

    private boolean isResultDetailKeyPresentInMemcache(String resultsKey) {
        boolean isResultKey = false;
        try {
            ResultDetailsWrapper resultDetailsWrapper = new ResultDetailsWrapper();
            resultDetailsWrapper.setKey(resultsKey);
            resultDetailsWrapper = (ResultDetailsWrapper) cacheClient.read(resultDetailsWrapper);
            if ( resultDetailsWrapper.getResults() != null ) {
                isResultKey = true;
            }
        } catch (Exception ex) {
            LOG.error("Unable to fetch result key: {} from Cached Server", resultsKey, ex);
        }
        return isResultKey;
    }

    private ResultDetailsWrapper buildResultsDTOData(com.drf.common.dto.results.TrackDTO track, RaceKeyDTO raceKeyDTO, List<String> trackDateList) {
        ResultDetailsWrapper resultDetailsWrapper = null;
        try {
            resultDetailsWrapper = new ResultDetailsWrapper();
            resultDetailsWrapper.setCountry(track.getCountry());
            resultDetailsWrapper.setTrackId(track.getId());
            resultDetailsWrapper.setTrackName(track.getName());

            ResultsDTO resultsDTO = dataHelper.getResultsDTO(raceKeyDTO);

            if ( resultsDTO == null ) {
                LOG.error("ResultsDTO Not available for track{}, on date {}", raceKeyDTO.getTrackId(), raceKeyDTO.getRaceDate().getDate());
                return null;
            }
            resultDetailsWrapper.setWeather(resultsDTO.getWeather());
            List<com.drf.common.dto.results.RaceDTO> races = resultsDTO.getRaces();

            List<ResultRaceDTOWrapper> raceDTOWrappers = new ArrayList<>();
            resultDetailsWrapper.setResults(raceDTOWrappers);

            EntryDTO entryDTO = dataHelper.getEntryDTO(raceKeyDTO);
            if ( entryDTO == null ) {
                LOG.error("EntryDTO not found for track {}, on date {}", raceKeyDTO.getTrackId(), raceKeyDTO.getRaceDate().getDate());
                return null;
            }

            for ( com.drf.common.dto.results.RaceDTO raceDTO : races ) {
                RaceKeyDTO raceKey = raceDTO.getRaceKey();
                try {
                    List<RaceDTO> racesOfEntries = entryDTO.getRaces();

                    for ( RaceDTO raceDTOOfEntries : racesOfEntries ) {
                        if ( raceKey.equals(raceDTOOfEntries.getRaceKey()) ) {
                            Map<String, HorseDTO> entriesHorseDetails = this.getEntriesRunnersDetails(raceDTOOfEntries.getHorses());
                            List<RunnerDTOWrapper> resultRunnerDTOList = new ArrayList<>();
                            for ( RunnerDTO resultsRunners : raceDTO.getRunners() ) {
                                RunnerDTOWrapper runnerDTOWrapper = new RunnerDTOWrapper(resultsRunners);
                                HorseDTO entriesHorseDTO = entriesHorseDetails.get(resultsRunners.getProgramNumber());
                                if ( entriesHorseDTO != null ) {
                                    com.drf.common.dto.pastperformance.HorseDTO ppHorseDTO = dataHelper.getHorseDTO(entriesHorseDTO.getRegistrationNumber());
                                    if ( ppHorseDTO != null ) {
                                        runnerDTOWrapper.setSireName(ppHorseDTO.sireName);
                                        runnerDTOWrapper.setBreederName(ppHorseDTO.breederName);
                                    } else {
                                        LOG.warn("Horse DTO information not found.");
                                    }
                                }
                                resultRunnerDTOList.add(runnerDTOWrapper);
                            }
                            raceDTO.setRunners(null);
                            List<PayOffDTOWrapper> payoffDTOWrapperList = new ArrayList<>(raceDTO.getPayoffs().size());
                            for ( PayoffDTO payoffDTO : raceDTO.getPayoffs() ) {
                                payoffDTOWrapperList.add(new PayOffDTOWrapper(payoffDTO));
                            }
                            raceDTO.setPayoffs(null);
                            ResultRaceDTOWrapper resultRaceDTOWrapper = new ResultRaceDTOWrapper(raceDTO, raceDTOOfEntries);
                            resultRaceDTOWrapper.setRunnerDTOs(resultRunnerDTOList);
                            resultRaceDTOWrapper.setPayoffDTOs(payoffDTOWrapperList);

                            raceDTOWrappers.add(resultRaceDTOWrapper);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Error in Building Result data", ex);
                }
            }
        } catch (Exception ex) {
            LOG.error("Error is deserilizing Entries DTO", ex);
        }
        return resultDetailsWrapper;
    }

    private Map<String, HorseDTO> getEntriesRunnersDetails(List<HorseDTO> horses) {
        Map<String, HorseDTO> horseMap = new HashMap<>();
        for ( HorseDTO horseDTO : horses ) {
            horseMap.put(horseDTO.getProgramNumber(), horseDTO);
        }
        return horseMap;
    }
}
