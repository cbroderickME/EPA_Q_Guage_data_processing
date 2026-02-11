################################################################################
## Program to download EPA gauge data (http://www.epa.ie/hydronet)
################################################################################

# clear datasets
rm(list = ls())
gc()

# set time
Sys.setenv(TZ = "GMT")

library(readxl)
library(lubridate)
library(statar)
library(httr)
library(rjson)
library(jsonlite)
library(dplyr)
library(stringr)
library(zip)
library(RANN)
library(here)
library(RCurl)
library(zoo)  # Needed for na.approx used in hourly interpolation

# as is listed on the epa website - there are other codes included in the actual datasets
quality_codes <- c(
  "Good, WR" = "Flow data derived from good quality water level records and within the limits of the rating curve",
  "Extrapolated, BL>" = "Flow data that has been extrapolated beyond the upper limit of the rating curve",
  "Extrapolated, BL<" = "Flow data that has been extrapolated beyond the lower limit of the rating curve",
  "Missing" = "Flow data is missing – either because there is no source water level data or because there is a period where the rating curve is not valid."
)

### set working directory
wkdir<-"K:/Fluvial/Hydrometric_Network/EPA_Gauges/Dwnld_LongTerm_Hydo_EPA"
wkdir<-"C:/Users/CBroderick/Downloads/EPA/wkdir"
setwd(wkdir)

### set working directory
setwd(wkdir)
dir.create(paste0(wkdir, "/../Q_Output_", Sys.Date()))
dir.create(paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/Gauges"))
dir.create(paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/log"))

### Get a list of all the potential gauges available from 
all_stats<-read_excel(paste0(wkdir, "/../Register-Hydrometric-stations-Ireland-2025.xlsx"))

EPA_stats <- all_stats %>%
  filter(
    `Responsible Body` == "Environmental Protection Agency",
    #`Station` == "Active",
    `Hydrometric Data Available` %in% c(
      "Water Level and Flow"
    )
  )

gauge_lst<-as.numeric(EPA_stats$`Station Number`)
gauge_lst_nme<-EPA_stats$`Station Name`

# save a list of the gauges with download data available
gauge_process_lst = matrix(NA, length(gauge_lst), 1)
gauge_process_lst_nme = matrix(NA, length(gauge_lst), 1)
gauge_process_log = matrix(NA, length(gauge_lst), 1)
gauge_process_abb = matrix(NA, length(gauge_lst), 1)

# run a loop to download all datasets from teh EPA website
# the programme works by attempting to download and appended the data for all stations
# if they are not processed successfully then they are assumed not to have one of the required datasets
# Information on whether a gauge was processed successfully is stored in a log file
for (val in seq(from=1, to=length(gauge_lst))){
  print((val/length(gauge_lst))*100)
  tryCatch({
    
    gauge_sel<-gauge_lst[val]
    print(gauge_sel)
    gauge_sel_adj<-str_pad(gauge_sel, 5, "pad"=0)
    gauge_process_lst[val]<-gauge_lst[val]
    gauge_process_lst_nme[val]<-gauge_lst_nme[val]
    
    dest_dir<-paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/Gauges/GaugeID_", gauge_sel)
    dir.create(dest_dir)
    setwd(dest_dir)
    
    ######## Download long term stage height above OD from HYDRO-DATA -----------------------------
    abb_lst=c("DUB","MON", "CON", "CAS", "KIK","LIM", "ATH")
    
    avail <- FALSE
    attempt <- 0
    while(isFALSE(avail) && attempt <= length(abb_lst)) {
      attempt <- attempt + 1
      address<-paste0("https://epawebapp.epa.ie/Hydronet/output/internet/stations/", abb_lst[attempt], "/", gauge_sel_adj, "/Q/complete_15min.zip")
      avail<-url.exists(address)
    }
    download.file(address, paste("Q_complete.zip", sep=""), silent=TRUE)
    gauge_process_abb[val]=abb_lst[attempt]
    
    ######## unzip and load file
    unzip("Q_complete.zip")
    
    complete<-read.csv("complete_15min.csv", sep=";", skip=6, header=TRUE)
    file.remove(list.files(pattern = glob2rx("complete_15min.csv")))
    names(complete) = c('Timestamp', 'Value', 'Quality.Code')
    
    ######## Convert datetime format
    complete$Timestamp = as.POSIXct(complete$Timestamp, tz = 'GMT')
    
    ######## sort data based on the datestamp
    comp_series_srt_id <- rev(order(as.numeric(complete$Timestamp)))
    comp_series_srt = complete[comp_series_srt_id,]
    
    ### set up datasets to write
    Discharge_raw<-comp_series_srt$Value
    Timestamp_raw <- comp_series_srt$Timestamp
    Quality_raw <- comp_series_srt$Quality.Code
    
    # create a dataframe with data to save
    comp_series_raw <- cbind.data.frame(Timestamp_raw, Discharge_raw, Quality_raw)
    
    print(unique(Quality_raw))
    
    #write out as RDS file
    saveRDS(comp_series_raw, 'Raw_Discharge.rds')
    write.csv(comp_series_raw, 'Raw_Discharge.csv', row.names = FALSE)
    
    ########################### PROCESS INTERPOLATED AND INFILLED DATASETS ######################################
    # Remove rows with missing values
    comp_series_raw <- comp_series_raw %>% filter(!is.na(Discharge_raw))
    
    # Remove rows where Quality_raw is in the exclude list
    exclude_codes <- c("Missing", "Unchecked", "Suspect", "Poor")
    comp_series_raw <- comp_series_raw %>%
      filter(!(Quality_raw %in% exclude_codes))
    
    ######## Actual time stamp
    act_ts = comp_series_raw$Timestamp_raw
    
    ######## create artifical time stamp (from 2024 to current time) and merge
    gen_ts = rev(seq(ymd_hm('2019-01-01 00:00', tz = "GMT"), Sys.time()+days(15), by = '15 mins'))
    gen_ts = lubridate::round_date(gen_ts, "15 minutes")
    
    # linearly interpolate
    y_query= approx(comp_series_raw$Timestamp_raw, comp_series_raw$Discharge_raw, gen_ts, method="linear", rule=1)
    
    # find the distance of each query point to the  nearest observed point in minutes
    x_query_nn<-nn2(data=comp_series_raw$Timestamp_raw, query=y_query$x, k=1)
    x_query_nn$nn.dists=x_query_nn$nn.dists/60
    
    # if the distance between the  query point and  the  nearest observed point is >15 mins then
    # recode the interpolated point as nan
    y_query_rm<-x_query_nn$nn.dists>15
    y_query$y[y_query_rm]=NA
    
    # tidy up
    comp_series_interp<-as.data.frame(cbind(y_query$x, y_query$y))
    names(comp_series_interp)<-c('Timestamp', 'Value')
    comp_series_interp<-as_tibble(comp_series_interp)
    comp_series_interp$Timestamp<-as.POSIXct(comp_series_interp$Timestamp, origin='1970-01-01')
    
    ### set up datasets to write
    Discharges_interp<-comp_series_interp$Value
    Timestamp_interp <- comp_series_interp$Timestamp
    
    # create a dataframe with data to save
    comp_series_interp <- cbind.data.frame(Timestamp_interp, Discharges_interp)
    
    #write out as RDS file
    saveRDS(comp_series_interp, 'Interp_Discharge.rds')
    write.csv(comp_series_interp, 'Interp_Discharge.csv', row.names = FALSE)
    
    ### HOURLY INTERPOLATION - Equivalent to Python's resample('60T').mean().interpolate(limit=6)
    
    # Convert the 15-min data to hourly mean values
    # require at least 2 valid (non-NA) 15-minute values per hour to compute the hourly mean—otherwise set that hour’s value to NA
    comp_series_hourly <- comp_series_interp %>%
      mutate(Timestamp_interp = lubridate::ceiling_date(Timestamp_interp, unit = "hour")) %>%
      group_by(Timestamp_interp) %>%
      summarise(
        n_values = sum(!is.na(Discharges_interp)),
        Value = if_else(n_values >= 2,
                        mean(Discharges_interp, na.rm = TRUE),
                        NA_real_)
      ) %>%
      select(-n_values) %>%  # remove helper column if you want
      ungroup()
    
    # Replace NaNs resulting from all-NA groups with NA
    comp_series_hourly$Value[is.nan(comp_series_hourly$Value)] <- NA
    
    # Interpolate only internal gaps (limit = 6), equivalent to `interpolate(limit=6, limit_area='inside')`
    comp_series_hourly$Value <- na.approx(comp_series_hourly$Value, maxgap = 6, na.rm = FALSE)
    
    # Save as RDS and CSV
    saveRDS(comp_series_hourly, 'Hourly_Interp_Discharge.rds')
    write.csv(comp_series_hourly, 'Hourly_Interp_Discharge.csv', row.names = FALSE)

    # save whether the gauge data was processed
    gauge_process_log[val] = TRUE 
  }, error=function(e){})
}

## write log file to show which gauges were processed
log_processed<-as.data.frame(cbind(unlist(gauge_process_lst), unlist(gauge_process_lst_nme), as.character(gauge_process_log), as.character(gauge_process_abb)))
names(log_processed)<-c("Gauge_ID",  "Gauge_Name",  "If_Processed", "ADD_CODE")
write.csv(log_processed, paste(wkdir, "/../Q_Output_", Sys.Date() ,"/log/Log_Processed_LTermObs_Q.csv", sep=""), row.names = FALSE)

