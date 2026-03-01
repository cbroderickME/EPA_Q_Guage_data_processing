################################################################################
## README – EPA Long-Term Discharge Data Downloader (OPW-style)
##
## Overview:
## Downloads long-term discharge data from EPA Hydronet.
## Produces:
##   - Raw 15-min data
##   - Interpolated 15-min series
##   - Hourly means (with missing-value thresholds)
##   - Daily means (00–00 and 09–09)
## Quality Code
## Description
## Good, WR Flow data derived from good quality water level records and within the limits of the rating curve
## Extrapolated, BL> Flow data that has been extrapolated beyond the upper limit of the rating curve
## Extrapolated, BL< Flow data that has been extrapolated beyond the lower limit of the rating curve
## Missing Flow data is missing – either because there is no source water level data or because there is a period where the rating curve is not valid.
##
## 2. Adjust settings:
##      - allowed_missing_hourly_pct: % missing allowed per hour
##      - allowed_missing_daily_pct: % missing allowed per day
##
## Logging:
## Records first valid timestamp, first regular 15-min block,
## start/end dates, proportion missing per interval.
##
## Requirements:
## R packages: readxl, lubridate, httr, dplyr, stringr, zoo, tibble, plotly, htmlwidgets
################################################################################

rm(list=ls()); gc()
Sys.setenv(TZ="GMT")

library(readxl)
library(lubridate)
library(httr)
library(dplyr)
library(stringr)
library(zoo)
library(tibble)
library(plotly)
library(htmlwidgets)

################################################################################
# USER SETTINGS
################################################################################
allowed_missing_hourly_pct <- 25
allowed_missing_daily_pct  <- 5
plot_gauge_id <- 1041   # Example gauge for plotting

################################################################################
# DERIVED THRESHOLDS
################################################################################
intervals_per_hour <- 4
intervals_per_day  <- 96
max_missing_hourly <- floor(intervals_per_hour * allowed_missing_hourly_pct / 100)
max_missing_daily  <- floor(intervals_per_day * allowed_missing_daily_pct / 100)

################################################################################
# WORKING DIRECTORY
################################################################################
wkdir <- "C:/Users/CBroderick/Downloads/EPA"
if(!dir.exists(wkdir)) dir.create(wkdir, recursive=TRUE)
setwd(wkdir)

out_dir <- file.path(wkdir, paste0("Q_Output_", Sys.Date()))
dir.create(out_dir, showWarnings=FALSE)
dir.create(file.path(out_dir,"Gauges"), showWarnings=FALSE)
dir.create(file.path(out_dir,"log"), showWarnings=FALSE)

################################################################################
# LOAD EPA STATION LIST
################################################################################
all_stats <- read_excel(file.path(wkdir,
                                  "/Register-Hydrometric-stations-Ireland-2025.xlsx"))

EPA_stats <- all_stats %>%
  filter(`Responsible Body`=="Environmental Protection Agency",
         `Hydrometric Data Available` %in% c("Water Level and Flow"))

gauge_lst <- as.numeric(EPA_stats$`Station Number`)
gauge_lst_nme <- EPA_stats$`Station Name`

################################################################################
# INITIALISE LOG
################################################################################
n <- length(gauge_lst)
log_tbl <- tibble(
  Gauge_ID = gauge_lst,
  Gauge_Name = gauge_lst_nme,
  Gauge_Abb = NA,
  LnkStatus_HD = NA,
  First_Valid_Timestamp = NA,
  First_Regular_15min_Timestamp = NA,
  Start_Date = NA,
  End_Date = NA,
  Prop_Missing_15min = NA,
  Prop_Missing_Hourly = NA,
  Prop_Missing_Daily_00_00 = NA,
  Prop_Missing_Daily_09_09 = NA,
  Data_Regularity = NA,
  Processed = NA
)


#gauge_lst=1016
################################################################################
# MAIN LOOP
################################################################################
abb_lst <- c("DUB","MON","CON","CAS","KIK","LIM","ATH")

for(val in seq_along(gauge_lst)){

 # val=1  
  gauge_sel <- gauge_lst[val]
  gauge_sel_adj <- str_pad(gauge_sel, 5, pad="0")
  cat("Processing gauge:", gauge_sel, "\n")
  
  dest_dir <- file.path(out_dir, "Gauges", paste0("GaugeID_", gauge_sel))
  dir.create(dest_dir, showWarnings=FALSE)
  
  # SKIP IF ALREADY PROCESSED
  if(file.exists(file.path(dest_dir,"Hourly_Discharge.rds"))){
    cat("Already processed — skipping.\n")
    log_tbl$Processed[val] <- "Skipped_existing_output"
    next
  }
  
  # FIND AVAILABLE ABB
  avail <- FALSE; attempt <- 0
  while(!avail && attempt < length(abb_lst)){
    attempt <- attempt + 1
    address <- paste0(
      "https://epawebapp.epa.ie/Hydronet/output/internet/stations/",
      abb_lst[attempt], "/", gauge_sel_adj, "/Q/complete_15min.zip"
    )
    res <- httr::HEAD(address)
    avail <- httr::status_code(res)==200
  }
  
  if(!avail){
    cat("No valid URL found.\n")
    log_tbl$Processed[val] <- "No_URL"
    next
  }
  
  log_tbl$Gauge_Abb[val] <- abb_lst[attempt]
  log_tbl$LnkStatus_HD[val] <- httr::status_code(res)
  
  tryCatch({
    # DOWNLOAD + READ
    download.file(address, file.path(dest_dir,"Discharge_complete.zip"), quiet=TRUE)
    unzip(file.path(dest_dir,"Discharge_complete.zip"), exdir=dest_dir)
    file.remove(file.path(dest_dir,"Discharge_complete.zip"))
    
    csv_file <- list.files(dest_dir, pattern="complete_15min.csv", full.names=TRUE)
    complete <- read.csv(csv_file, sep=";", skip=6)
    file.remove(csv_file)
    
    names(complete) <- c("Timestamp","Discharge","Quality")
    complete$Timestamp <- as.POSIXct(complete$Timestamp, tz="GMT")
    
    # FILTER GOOD QUALITY
    comp_raw <- complete %>%
      filter(!is.na(Discharge), Quality %in% c("Good","Extrapolated","Unchecked","Fair"))
    
    # LOG FIRST VALID
    log_tbl$First_Valid_Timestamp[val] <- min(comp_raw$Timestamp)
    
    # FIRST REGULAR 15-MIN BLOCK
    dt_diff <- diff(as.numeric(comp_raw$Timestamp))/60
    is_15 <- dt_diff==15
    rle_15 <- rle(is_15)
    idx_run <- which(rle_15$values & rle_15$lengths>=4)
    if(length(idx_run)==0){log_tbl$Processed[val]<-"No_Regular_Block"; next}
    run_start <- sum(rle_15$lengths[seq_len(idx_run[1]-1)])+1
    log_tbl$First_Regular_15min_Timestamp[val] <- comp_raw$Timestamp[run_start]
    
    start_time <- min(comp_raw$Timestamp, na.rm = TRUE)
    end_time   <- max(comp_raw$Timestamp, na.rm = TRUE)
    log_tbl$Start_Date[val] <- start_time
    log_tbl$End_Date[val] <- end_time
    
    # 15-min full series
    full_ts <- seq(start_time, end_time, by = "15 min")
    comp_full <- tibble(Timestamp = full_ts) %>%
      left_join(
        comp_raw %>%
          select(Timestamp, Discharge) %>%
          group_by(Timestamp) %>% slice(1) %>% ungroup(),
        by = "Timestamp"
      )
    comp_full$Discharge <- zoo::na.approx(comp_full$Discharge, x=comp_full$Timestamp,
                                          maxgap=1, na.rm=FALSE)
    
    log_tbl$Prop_Missing_15min[val] <- mean(is.na(comp_full$Discharge))
    
    # HOURLY AGGREGATION
    hourly <- comp_full %>%
      mutate(Hour=floor_date(Timestamp,"hour")) %>%
      group_by(Hour) %>%
      summarise(
        n_total=n(),
        n_valid=sum(!is.na(Discharge)),
        Discharge=if_else((n_total-n_valid)<=max_missing_hourly,
                          mean(Discharge, na.rm=TRUE), NA_real_)
      ) %>% ungroup()
    log_tbl$Prop_Missing_Hourly[val] <- mean(is.na(hourly$Discharge))
    
    # DAILY 00-00
    daily_00 <- comp_full %>%
      mutate(Date=as.Date(Timestamp)) %>%
      group_by(Date) %>%
      summarise(
        n_total=n(),
        n_valid=sum(!is.na(Discharge)),
        Discharge=if_else((n_total-n_valid)<=max_missing_daily,
                          mean(Discharge, na.rm=TRUE), NA_real_)
      ) %>% ungroup()
    log_tbl$Prop_Missing_Daily_00_00[val] <- mean(is.na(daily_00$Discharge))
    
    # DAILY 09-09
    daily_09 <- comp_full %>%
      mutate(Date=as.Date(Timestamp - hours(9))) %>%
      group_by(Date) %>%
      summarise(
        n_total=n(),
        n_valid=sum(!is.na(Discharge)),
        Discharge=if_else((n_total-n_valid)<=max_missing_daily,
                          mean(Discharge, na.rm=TRUE), NA_real_)
      ) %>% ungroup()
    log_tbl$Prop_Missing_Daily_09_09[val] <- mean(is.na(daily_09$Discharge))
    
    # SAVE RDS
    saveRDS(comp_raw, file.path(dest_dir,"Raw_Discharge.rds"))
    saveRDS(comp_full, file.path(dest_dir,"Regular_15min_Discharge.rds"))
    saveRDS(hourly, file.path(dest_dir,"Hourly_Discharge.rds"))
    saveRDS(daily_00, file.path(dest_dir,"Daily_Discharge_00_00.rds"))
    saveRDS(daily_09, file.path(dest_dir,"Daily_Discharge_09_09.rds"))
    
    log_tbl$Data_Regularity[val] <- ifelse(all(dt_diff==15),"Regular","Irregular")
    log_tbl$Processed[val] <- TRUE
    
    ################################################################################
    # OPTIONAL: PLOT LAST YEAR FROM LAST VALID RECORD
    ################################################################################
    start_plot <- end_time - 365*24*60*60  # last 365 days from last valid timestamp
    fig <- plot_ly()
    
    if(nrow(subset(comp_raw, Timestamp>=start_plot))>0){
      plot_raw <- subset(comp_raw, Timestamp>=start_plot)
      fig <- fig %>% add_lines(x=plot_raw$Timestamp, y=plot_raw$Discharge, name="Raw",
                               line=list(color='gray', width=1))
    }
    if(nrow(subset(comp_full, Timestamp>=start_plot))>0){
      plot_full <- subset(comp_full, Timestamp>=start_plot)
      fig <- fig %>% add_lines(x=plot_full$Timestamp, y=plot_full$Discharge, name="15-min",
                               line=list(color='blue', width=1.5))
    }
    if(nrow(subset(hourly, Hour>=start_plot))>0){
      plot_hourly <- subset(hourly, Hour>=start_plot)
      fig <- fig %>% add_lines(x=plot_hourly$Hour, y=plot_hourly$Discharge, name="Hourly",
                               line=list(color='green', width=2))
    }
    if(nrow(subset(daily_00, Date>=as.Date(start_plot)))>0){
      plot_daily00 <- subset(daily_00, Date>=as.Date(start_plot))
      fig <- fig %>% add_lines(x=plot_daily00$Date, y=plot_daily00$Discharge, name="Daily 00-00",
                               line=list(color='red', width=2, dash='dash'))
    }
    if(nrow(subset(daily_09, Date>=as.Date(start_plot)))>0){
      plot_daily09 <- subset(daily_09, Date>=as.Date(start_plot))
      fig <- fig %>% add_lines(x=plot_daily09$Date, y=plot_daily09$Discharge, name="Daily 09-09",
                               line=list(color='orange', width=2, dash='dot'))
    }
    
    #  save widget
    fig <- fig %>% layout(title=paste("Discharge - Gauge", gauge_sel),
                          xaxis=list(title="Date"),
                          yaxis=list(title="Discharge (m³/s)"))
    htmlwidgets::saveWidget(fig, file.path(dest_dir, paste0("Gauge_",gauge_sel,"_Year_from_end_date.html")))
    
  }, error=function(e){
    cat("Error processing gauge", gauge_sel, ":", e$message, "\n")
    log_tbl$Processed[val] <- "Error"
  })
}

################################################################################
# SAVE LOG
################################################################################

log_tbl_out <- log_tbl %>%
  mutate(
    First_Valid_Timestamp = if (!all(is.na(First_Valid_Timestamp))) {
      format(as.POSIXct(First_Valid_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    First_Regular_15min_Timestamp = if (!all(is.na(First_Regular_15min_Timestamp))) {
      format(as.POSIXct(First_Regular_15min_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    Start_Date = if (!all(is.na(Start_Date))) {
      format(as.POSIXct(Start_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    End_Date = if (!all(is.na(End_Date))) {
      format(as.POSIXct(End_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ }
  )

write.csv(log_tbl_out, file.path(out_dir,"log","Log_Processed_LTermObs_Q.csv"), row.names=FALSE)
cat("Processing complete.\n")