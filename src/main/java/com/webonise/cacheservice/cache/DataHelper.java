package com.webonise.cacheservice.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.drf.common.dto.cardselector.CardSelectorDTO;
import com.drf.common.dto.cardselector.TrackDTO;
import com.drf.common.dto.chart.ChartDTO;
import com.drf.common.dto.common.DayDTO;
import com.drf.common.dto.common.RaceKeyDTO;
import com.drf.common.dto.entry.EntryDTO;
import com.drf.common.dto.entry.MainMenuDTO;
import com.drf.common.dto.pastperformance.CloserLooksDTO;
import com.drf.common.dto.pastperformance.HorseDTO;
import com.drf.common.dto.pastperformance.RaceDTO;
import com.drf.common.dto.racecondition.RaceConditionDTO;
import com.drf.common.dto.results.ResultsDTO;
import com.drf.common.dto.sibling.SiblingSummaryDTO;
import com.drf.common.dto.trainerpattern.TrainerDTO;
import com.drf.common.util.FileHelper;

import drf.common.wrappers.entries.ChangesTrackDTO;

@Component
public class DataHelper {
    private static final String DATE_FORMAT = "MM/dd/yyyy";
    private static final String _ = "_";
    private static final String TURF = "Turf";
    private static final String TURF_TRACK_CUTS = "Turf-TrackCuts/";
    private static final String FORMULATOR_WEB = "formulatorWeb/";
    private static final String OPUS_RESOURCES_IMAGES_TRACK_CUT_MASTER = "opus/resources/images/Track_Cut_Master/";
    private static final String LINE_SEPARATOR = "line.separator";
    private static final String PDF_EXTENSION = ".pdf";
    private static final String PROPICK = "propick";
    private static final String PROPICK_PDF = "ProPick/PDF/";

    private static final Logger LOG = LoggerFactory.getLogger(DataHelper.class);

    private String formulatorFilePathRoot;
    private String dtoBaseFilePath;
    private String trackDiagramsPathRoot;
    private String propickPdfPathRoot;

    private SimpleDateFormat dateFormat;

    @Value("${pp.dto.basePath}")
    public void setFormulatorFilePathRoot(String filePathRoot) {
        this.formulatorFilePathRoot = filePathRoot + FORMULATOR_WEB;
        this.dtoBaseFilePath = filePathRoot;
        this.trackDiagramsPathRoot = filePathRoot + OPUS_RESOURCES_IMAGES_TRACK_CUT_MASTER;
        this.propickPdfPathRoot = filePathRoot + PROPICK_PDF;
        dateFormat = new SimpleDateFormat(DATE_FORMAT);
    }

    /**
     * Fetches all the tracks
     * 
     * @return
     * @throws Exception
     */
    public List<TrackDTO> getTracks() throws Exception {
        CardSelectorDTO tracksDTO = null;
        List<TrackDTO> tracks = null;
        String fileName = null;
        try {
            fileName = FileHelper.getCardSelector();
            tracksDTO = (CardSelectorDTO) readFile(formulatorFilePathRoot + fileName);
            tracks = tracksDTO.tracks;
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the file name for tracks", e);
            throw new Exception(e.getMessage());
        }
        return tracks;
    }

    /**
     * Fetches the Race Condition info
     * 
     * @param raceKey
     *            Race Key
     * @return
     * @throws Exception
     */
    public RaceConditionDTO getRaceCondition(RaceKeyDTO raceKey) throws Exception {
        String fileName = null;
        try {
            fileName = FileHelper.getRaceConditionDTOFilePath(raceKey);
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the file name for race conditions", e);
            throw new Exception(e.getMessage());
        }
        return (RaceConditionDTO) readFile(formulatorFilePathRoot + fileName);
    }

    /**
     * Fetches the Race Details for the passed info
     * 
     * @param trackId
     *            Selected Track
     * @param raceDate
     *            Race Date
     * @param raceNumber
     *            Race Number
     * @param dayEvening
     *            Day or Evening Indicator
     * @param country
     *            Country
     * @return RaceDTO
     * @throws Exception
     */
    public RaceDTO getRaceDetail(String trackId, DayDTO raceDate, int raceNumber, String dayEvening, String country) throws Exception {
        String fileName = "";
        try {
            fileName = FileHelper.getRaceDTOFilePath(trackId, raceDate, raceNumber, dayEvening, country);
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the file name for getting race details", e);
            throw new Exception(e.getMessage());
        }
        return (RaceDTO) readFile(formulatorFilePathRoot + fileName);
    }

    /**
     * Gets the closer looks for horses in a race
     * 
     * @param trackId
     * @param raceDate
     * @param raceNumber
     * @param dayEvening
     * @param country
     * @return
     * @throws Exception
     */
    public CloserLooksDTO getCloserLooks(String trackId, DayDTO raceDate, int raceNumber, String dayEvening, String country) throws Exception {
        CloserLooksDTO closerLooks = null;
        String fileName = "";
        try {
            fileName = FileHelper.getCloserLooksFilePath(trackId, raceDate.getDate(), raceNumber, dayEvening, country);
            closerLooks = (CloserLooksDTO) readFile(formulatorFilePathRoot + fileName);
        } catch (Exception e) {
            LOG.error("Closer looks dont exist for {} {} {} {} {}", trackId, raceDate);

        }
        return closerLooks;
    }

    /**
     * 
     * @param trainerId
     * @param trainerType
     * @return
     * @throws Exception
     */
    public TrainerDTO getTrainerPattern(Integer trainerId, String trainerType) throws Exception {
        String fileName = "";
        try {
            fileName = FileHelper.getTrainerDTOFilePath(trainerId, trainerType);
        } catch (Exception e) {

            LOG.error("Error occured while retrieving the file name for getting race TrainerDTO");
            throw new Exception(e.getMessage());
        }
        return (TrainerDTO) readFile(formulatorFilePathRoot + fileName);
    }

    /**
     * provides the horseDetails
     * 
     * @param number
     * @return
     * @throws Exception
     */
    public HorseDTO getHorseDTO(String number) throws Exception {
        String fileName = "";
        try {
            fileName = FileHelper.getHorseDTOFilePath(number);
        } catch (Exception e) {

            LOG.error("Error occured while retrieving the file name for getting race horseDTO");
            throw new Exception(e.getMessage());
        }
        return (HorseDTO) readFile(formulatorFilePathRoot + fileName);
    }

    public List<ChartDTO> getCharts(String trackId, DayDTO raceDate, int raceNumber, String dayEvening, String country) throws Exception {
        List<ChartDTO> charts = new ArrayList<>();
        ChartDTO chart = null;
        String fullfilePath = formulatorFilePathRoot + FileHelper.getChartDTOFilePath(trackId, raceDate, raceNumber, dayEvening, country);
        File file = new File(fullfilePath);
        if ( file.exists() ) {
            String fileName = file.getName();
            File baseDir = new File(file.getParentFile().toString());
            String BaseFile = fileName.substring(0, fileName.length() - 6) + "*";
            FileFilter fileFilter = new WildcardFileFilter(BaseFile);
            File[] files = baseDir.listFiles(fileFilter);

            for ( int i = 0; i < files.length; i++ ) {
                // chart = (ChartDTO)
                // readFile(files[i].getPath().replaceFirst(formulatorFilePathRoot,
                // ""));
                chart = (ChartDTO) readFile(files[i].getPath());
                charts.add(chart);
            }
        } else {
            LOG.error("File does not exist: {}", fullfilePath);
            throw new Exception("file does not exist");
        }
        return charts;
    }

    public String getTrackdiagram(String fileName, String trackType) throws Exception {
        String path = trackDiagramsPathRoot + fileName;
        CharSequence trackTypeSequence = TURF;
        if ( trackType.contains(trackTypeSequence) ) {
            path = dtoBaseFilePath + TURF_TRACK_CUTS + fileName;
        }
        File file = new File(path);
        String diagram = null;
        try {
            FileInputStream imageInFile;
            imageInFile = new FileInputStream(file);
            byte imageData[] = new byte[(int) file.length()];
            imageInFile.read(imageData);
            diagram = new String(Base64.encodeBase64(imageData, false));
            imageInFile.close();
        } catch (Exception e) {
            LOG.error("file {} does not exist", trackDiagramsPathRoot + fileName);
            throw new Exception("file does not exist", e);
        }
        return diagram;
    }

    /**
     * @param filePath
     * @return
     * @throws Exception
     */
    private Object readFile(String filePath) throws Exception {
        Object objectStream = null;
        ObjectInputStream ois = null;
        FileInputStream fin = null;
        if ( new File(filePath).exists() ) {
            try {
                if ( filePath != null ) {
                    fin = new FileInputStream(filePath);
                } else {
                    throw new Exception("Error occured while reading the serialised file");
                }
                if ( fin != null )
                    ois = new ObjectInputStream(fin);
                objectStream = ois.readObject();
            } catch (IOException | ClassNotFoundException ex) {
                LOG.error("Error occured while reading the serialised file", ex);
                throw new Exception(ex.getMessage());
            } finally {
                try {
                    if ( ois != null )
                        ois.close();
                } catch (IOException e) {
                    LOG.error("Error occured while closing the file output stream", e);
                    throw new Exception(e.getMessage());
                }
            }
        } else {
            LOG.error("file {} does not exist", filePath);
            // throw new Exception("File not Found");
        }
        return objectStream;
    }

    /**
     * This method read the Entries for DA & PRO
     * 
     * @return
     * @throws Exception
     */
    public List<com.drf.common.dto.entry.TrackDTO> getEntries() throws Exception {
        MainMenuDTO menuDTO = null;
        List<com.drf.common.dto.entry.TrackDTO> tracks = null;
        String fileName = null;
        try {
            fileName = FileHelper.getEntriesMainMenu();
            menuDTO = (MainMenuDTO) readFile(dtoBaseFilePath + fileName);
            tracks = menuDTO.getTracks();
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the file name for entries", e);
            throw new Exception(e.getMessage());
        }
        return tracks;
    }

    /**
     * This method read the Entry DTO for DA & PRO
     * 
     * @param raceKey
     * @return
     * @throws Exception
     */
    public EntryDTO getEntryDTO(RaceKeyDTO raceKey) throws Exception {
        EntryDTO entryDTO = null;
        String fileName = null;
        try {
            fileName = FileHelper.getEntriesDTOFilePath(raceKey);
            entryDTO = (EntryDTO) readFile(dtoBaseFilePath + fileName);
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the entry DTO", e);
            throw new Exception(e.getMessage());
        }
        return entryDTO;
    }

    /**
     * This method read the Changes TrackDTO for DA & PRO
     * 
     * @param raceKey
     * @return
     * @throws Exception
     */
    public ChangesTrackDTO getChangesTrackDTO(RaceKeyDTO raceKey) throws Exception {
        ChangesTrackDTO changesTrackDTO = new ChangesTrackDTO();
        String fileName = null;
        try {
            fileName = getChangesTrackDTOFileName(raceKey);

            Object readFile = readFile(dtoBaseFilePath + fileName);
            if ( readFile instanceof com.drf.common.dto.changes.TrackDTO ) {
                changesTrackDTO.setChanges((com.drf.common.dto.changes.TrackDTO)readFile);
            }
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the Changes TrackDTO", e);
            throw new Exception(e.getMessage());
        }
        return changesTrackDTO;
    }

    /**
     * Return file name of ChangesDTO
     * 
     * @param raceKey
     * @return
     */
    public String getChangesTrackDTOFileName(RaceKeyDTO raceKey) {
        String fileName;
        fileName = FileHelper.getEntriesDTOFilePath(raceKey);
        fileName = fileName.replaceAll("Card", "Changes");
        return fileName;
    }

    /**
     * This method read the Result Entries for DA & PRO
     * 
     * @return
     * @throws Exception
     */
    public List<com.drf.common.dto.results.TrackDTO> getResults() throws Exception {
        com.drf.common.dto.results.MainMenuDTO menuDTO = null;
        List<com.drf.common.dto.results.TrackDTO> tracks = null;
        String fileName = null;
        try {
            fileName = FileHelper.getResultsMainMenu();
            menuDTO = (com.drf.common.dto.results.MainMenuDTO) readFile(dtoBaseFilePath + fileName);
            tracks = menuDTO.getTracks();
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the file name for results", e);
            throw new Exception(e.getMessage());
        }
        return tracks;
    }

    /**
     * This method read result DTO for DA & PRO
     * 
     * @param raceKey
     * @return
     * @throws Exception
     */
    public ResultsDTO getResultsDTO(RaceKeyDTO raceKey) throws Exception {
        ResultsDTO resultsDTO = null;
        String fileName = null;
        try {
            fileName = FileHelper.getResultsDTOFilePath(raceKey);
            resultsDTO = (ResultsDTO) readFile(dtoBaseFilePath + fileName);
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the results DTO", e);
            throw new Exception(e.getMessage());
        }
        return resultsDTO;
    }
    
    /**
     * This file read the content of file into String
     * 
     * @param file
     * @return
     * @throws IOException
     */
    public String readXMLFile(String file) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(formulatorFilePathRoot + file));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty(LINE_SEPARATOR);

        while ( (line = reader.readLine()) != null ) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }
    
    /**
     * Generates fileName of pdf file
     * 
     * @param trackId
     * @param country
     * @param raceDate
     * @param dayEvening
     * @return String
     */
    public String getPdfFileName(String trackId, String country, String raceDate, String dayEvening) {
        StringBuilder fileNameBuilder = new StringBuilder();
        fileNameBuilder.append(PROPICK);
        fileNameBuilder.append(trackId.length() == 2 ? trackId + _ : trackId);
        fileNameBuilder.append(country);
        fileNameBuilder.append(raceDate);
        fileNameBuilder.append(dayEvening);
        fileNameBuilder.append(PDF_EXTENSION);
        return fileNameBuilder.toString();
    }

    /**
     * Fetches siblings
     * 
     * @param raceNumber
     * @param dayEve
     * @param raceDate
     * @param country
     * 
     * @return
     * @throws Exception
     */
    public SiblingSummaryDTO getSiblings(String trackId, String country, String raceDate, String dayEve, Integer raceNumber) throws Exception {
        SiblingSummaryDTO siblingSummaryDTO = null;
        try {
            Date date = (Date) dateFormat.parse(raceDate);
            String fileName = FileHelper.getSiblingSummaryDTOFilePath(new RaceKeyDTO(trackId, country, new DayDTO(date), dayEve, raceNumber));
            siblingSummaryDTO = (SiblingSummaryDTO) readFile(formulatorFilePathRoot + fileName);
        } catch (Exception e) {
            LOG.error("Error occured while retrieving the file name for siblings", e);
            throw new Exception(e.getMessage());
        }
        return siblingSummaryDTO;
    }
}
