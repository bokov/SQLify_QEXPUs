#' ---
#' title: "QEXPU Multi Table Import"
#' abstract: | 
#' 
#'  CMS ships quarterly reports to ACOs as zip files containing multiple files
#'  in multiple formats with row and column names/locations that can sometimes
#'  change. In short, these packages are not designed to for optimal machine
#'  readability. Yet an ACO needs a consistent tabular format for current and
#'  historical benchmarks, truncations, KPIs, etc. so these can be easily
#'  integrated into existing databases and dashboards. This script cranks
#'  through all available such zip files for QEXPU and BNMRK files (located in
#'  the .expuzipdir and .bnmrkzipdir, which you should override with your own
#'  paths). It pulls out specific workbooks, sheets, and ranges, standardizes
#'  the names, and creates three unified data.frames: risknormvals (the
#'  eligibility-class-specific renormalization factors for each reporting period
#'  represented in your files), dat40 (almost every item from the Table-1
#'  Aggregate EU reports of all your QEXPU files in wide format with both the
#'  messy original-ish variable names identical data in columns with cleaned up
#'  snake-case names, indexed by RQ which is QEXPU report quarter), and dat50
#'  (dat40 but pivoted so that the QEXPU_Name has all the snake-case variable
#'  names and QEXPU_Value has the corresponding values with RQ repeated so that
#'  RQ and QEXPU_Name together form the primary key). These tables then get
#'  exported to .sql scripts ready to open in your database client and create
#'  lookup tables. Currently this is targeted at MS SQLServer but I'm sure it
#'  can be adapted to other RDBMSs without too much trouble. The generated sql
#'  scripts-- Lookup_RiskNormVals.sql, Lookup_QEXPU.sql,  and
#'  Lookup_TallQEXPU.sql -- each write to a temporary table named ##sqlifyoutput
#'  from which you can then select into whatever permanent table you want. Make
#'  sure to drop it before running another of the .sql scripts, or manually edit
#'  each one to have a different name for the target table. Or, you can export
#'  as csv or tsv any of risnormvals (see above), dat30 (QEXPU variables with
#'  messy names), dat30a (QEXPU variables with snake-case names), dat40 (see
#'  above, a join of dat30 and dat30a), or dat50 (a pivot of dat40, see above).
#'  
#'  This is still a work in progress, but I hope this is useful to you.
#'  
#' author:  
#' - "Alex F. Bokov^[UT Health, Department of Population Health Sciences]"
#' date: "`r Sys.Date()`"
#' output:
#'   html_document:
#'     keep_md: true
#'     toc: true
#'     toc_float: true
#'     code_folding: hide
#' ---
#'
#+ message=F,echo=F
# init ----
.debug <- 1;
.blurb <- '';
.note <- '';
.ranseed <- 20230922;
knitr::opts_chunk$set(echo=.debug>0, warning=.debug>1, message=.debug>1);
options(datatable.na.strings=c('N/A','NULL',''));
options(datatable.integer64="numeric");


library(purrr); # for doing more stuff with dynamic functions 
library(dplyr); library(ggplot2); library(rio); library(forcats); #library(ggridges);
#library(gridExtra); library(cowplot); # additional ggplot features
#library(pander);
library(stringr);
#library(boot);
#panderOptions('big.mark',','); panderOptions("table.split.table",Inf); panderOptions("table.split.cells",Inf); panderOptions('missing','-');

source('functions.R');

fmn <- scales::comma_format();
qexpupipeline <- . %>% na.omit %>%
  mutate(rowname=sub('\\[[0-9]{1,2}\\]$','',rowname) %>%
           sub('-[Tt]erm',' Term',.) %>%
           sub('Person-Years','Person Years',.) %>%
           ifelse(row_number()<48
                  ,sub('Term Acute Care|Term Care','Term Stay',.),.) %>%
           trimws() %>%
           make.unique()) %>%
  mutate(rowname=case_when(
    rowname %in% c("Total","End Stage Renal Disease"
                   ,"Disabled","Aged/Dual","Aged/Non-Dual") ~
      paste('PY',rowname)
    ,rowname %in% c("Total.1","End Stage Renal Disease.1","Disabled.1"
                    ,"Aged/Dual.1","Aged/Non-Dual.1") ~
      paste('PMPY',sub('.1$','',rowname))
    ,rowname %in% c('Person Years'
                    ,'Total Expenditures per Assigned Beneficiary') ~
      paste('Declined',rowname)
    ,rowname == 'Person Years.1' ~ 'Non Claims Person Years'
    ,TRUE ~ rowname
  )) %>%
  subset(!grepl('^NA|^\\[|NOTES:|Quarter|Number of ACOs',rowname));



.tmpdir <- '/tmp';
.outputdir <- .tmpdir;
.expuzipdir <- file.path('~','Documents','ACO','QEXPUzips',''); 
.bnmrkzipdir <- file.path('~','Documents','ACO','BNMRK','');
.thisenv <- environment();
qexpu_temp <- file.path(.outputdir,'qexpu_temp');
bnmrk_temp <- file.path(qexpu_temp,'bnmrk');
#' To override the file names and paths in `outputscriptpath`,`qexpudir`,
#' `qexpus`, and `bnmrks` create a file in the same directory as this script
#' named `local.confi.R` and in that file create your own versions of the above
#' variables. It will be run if it exists.
if(file.exists('local.config.R')) source('local.config.R');


# import QEXPUs ----

# unzip all available QEXPU and BNMRK files
.junk <- list.files(.expuzipdir,full.names = T) %>% 
  sapply(unziprecursive,outdir = qexpu_temp,printfiles=F);

# get the top-level folders
qexpu_toplevel <- list.files(qexpu_temp,full.names = T);


# labels top-level folders for all report periods in the 20XXQX format
names(qexpu_toplevel) <- qexpu_toplevel %>% {
  case_when(
    # EOY settlement files get assigned a fictional quarter Q5 for their respective years
    grepl('STLMT',.) ~ str_replace(., ".*STLMT\\.D([0-9]{2}).*", "20\\1Q5")
    # for recent QEXPUs, the quarter is in the final segment of the file name after T
    ,grepl('9999',.) ~ str_replace(., ".*QEXPU\\.D([0-9]{2})9999\\.T0([0-9]).*", "20\\1Q\\2")
    # for old QEXPUs, the month and day are given instead of quarter
    ,grepl("QEXPU\\.D([0-9]{2})([0-9]{4})",.) ~ {
      # Extract year and month
      matches <- str_match(., "QEXPU\\.D([0-9]{2})([0-9]{4})");
      year <- matches[,2];
      month <- as.numeric(substr(matches[,3], 1, 2));
      # convert month to quarter
      quarter_val <- lubridate::quarter(ifelse(month %in% 1:12,month,1)) # Use lubridate::quarter()
      paste0("20", year, "Q", quarter_val)}
    ,TRUE ~ .
  )};



# normalization factors ----
# find risk normalization values for each reporting period and enrollment type
risknormvals <- sapply(qexpu_toplevel,list.files
                       ,patt='ALR\\s?Parameters\\.xlsx',full=T,recursive=T) %>% 
  lapply(function(xx) try(import(xx,which='Risk Score Parameters') %>% 
                            setNames(c('Class','HCC_norm','Demog_norm')))) %>% 
  Filter(function(xx) !is(xx,'try-error'),.) %>% 
  bind_rows(.id='ReportPeriod') %>% 
  mutate(Class=case_match(Class,'Disabled'~'DSBL','Aged/Dual'~'AGDU','Aged/Non-Dual'~'AGND','ESRD'~'ESRD')) %>% 
  tidyr::pivot_wider(names_from = Class,values_from = c('HCC_norm','Demog_norm'));

# QEXPU info ----
lookupcomponents <- import('data/Lookup_ComponentCrosswalk.csv');

qexpupaths <- sapply(qexpu_toplevel,list.files,patt='QEXPU.*\\.xlsx$',full=T,recursive=T) %>% unlist;
qexputab1<-sapply(qexpupaths,function(xx) readxl::excel_sheets(xx) %>%
                    grep('1.*Aggregate.EU.Report$',.,val=T)) %>% unlist;
qexputab1a <- sapply(qexpupaths,function(xx) readxl::excel_sheets(xx) %>%
                       grep('1A.*COVID',.,val=T)) %>% unlist();

# main qexpu ----
dat0 <- dat0b <- dat0c <- dat1 <- list();
for(ii in names(qexpupaths)){
  # QEXPU rolling 12 totals
  dat0ii <- import(qexpupaths[ii],which=qexputab1[ii]
                   ,col_types=c('text','numeric','numeric','numeric'),range="A1:D100"
                   ,col_names=c('rowname',paste0(ii,c('','_allacos','_national')))) %>%
    qexpupipeline;
  dat0[[ii]] <- dat0ii[,1:2];
  dat0b[[ii]] <-subset(dat0ii,grepl('^Emergency.Department.Visits$|^Short.Term.Stay.Hospital.1',rowname));
  # QEXPU rolling 12 totals, COVID excluded
  if(ii %in% names(qexputab1a)){
    dat0c[[ii]] <- import(qexpupaths[ii],which=qexputab1a[ii]
                          ,col_types=c('text','numeric'),range="A1:B100"
                          ,col_names=c('rowname',ii)) %>% qexpupipeline;}
  # Parameters & Truncation
  dat1[[ii]] <- import(qexpupaths[ii],which='Parameters'
                       ,col_types=c('text','numeric'),range="A1:B90"
                       ,col_names=c('rowname',ii)) %>% na.omit %>% 
    mutate(rowname=make.unique(rowname) %>% 
             sub('\\.1$',' Excluding COVID',.) %>% 
             ifelse(. %in% c('Claims Completion Factor'
                             ,'Performance Year Participant List on which Report is Based')
                    ,.,paste('Truncation',.)))
};

# import BNMRKs ----

# extract, locate, and import all available BNMRK files
.junk <-  list.files(.bnmrkzipdir,pattern='*.zip',full.names = T) %>% 
  sapply(unziprecursive,outdir = bnmrk_temp,printfiles=F);

bnmrk_files <- list.files(bnmrk_temp,recursive = T,full.names = T,pattern='BNMRK.*\\.xlsx') %>% 
  setNames(.,str_replace(.,'.*D([0-9]{2}).*','20\\1'));

bnmrksq<-list(); for(ii in names(bnmrk_files)) for(jj in intersect(paste0(ii,'Q',1:4),names(qexpupaths))){
  bnmrksq[[jj]] <- bnmrk_files[[ii]]};

#' ### Yearly benchmark info
# yearly benchmarks ----
dat2 <- sapply(names(bnmrksq),function(ii) {
  # dynamically deduce the range containing the benchmark info
  iirange <- import(bnmrksq[[ii]],which='Table 1 - Historical Benchmark'
                    ,col_names=F,na=c('-','',"–"))[,1] %>%
    grep("(ESRD|Disabled|Aged/dual|Aged/non-dual)[ ]*$",.) %>% 
    max(na.rm=T) %>% {sprintf('A%d:E%d',.-3,.+1)};
  import(bnmrksq[[ii]],which='Table 1 - Historical Benchmark'
         ,col_names=F,range=iirange,na=c('-','',"–")) %>% 
    setNames(c('rowname','BY1','BY2','BY3',ii)) %>% dplyr::select(c('rowname',ii)) %>%
    mutate(.,rowname = ifelse(grepl('Historic',rowname)
                              ,'Benchmark_Overall'
                              ,paste0('Benchmark_',rowname))) #%>% print
},simplify=F) %>% Reduce(full_join,.);



#' ## Join the years into one table each for QEXPU, params, and benchmarks
# joins ----
dat10 <- Reduce(full_join,dat0);
dat10c <- Reduce(full_join,dat0c) %>% mutate(rowname=paste(rowname,'Excluding COVID'));
dat11 <- Reduce(full_join,dat1);
dat20 <- bind_rows(dat10,dat10c,dat11,dat2);

#' ## Clean up variable names
# clean up ----
dat20$rowname <- gsub('/',' ',dat20$rowname) %>% gsub('[.]',' ',.);
#' ## Transpose so each row is a year and more backwards compatibility renames
dat30 <- dplyr::select(dat20,-'rowname') %>% t %>% data.frame %>% 
  setNames(dat20$rowname) %>% tibble::rownames_to_column('RQ') %>% 
  mutate(#RQ=paste0('20',RQ)
    # This part converts the per 1000 PY to actual number of events
    `STAC Actual Count`=as.integer(`Short Term Stay Hospital 1`*`PY Total`/1000)
    ,`LTAC Actual Count`=as.integer(`Long Term Stay Hospital 1`*`PY Total`/1000)
    ,`Rehab Actual Count`=as.integer(`Rehabilitation Hospital or Unit 1`*`PY Total`/1000)
    ,`Psych Actual Count`=as.integer(`Psychiatric Hospital or Unit 1`*`PY Total`/1000)
    ,`ED Actual Count`=as.integer(`Emergency Department Visits`*`PY Total`/1000)
    ,`ED to IP Actual Count`=as.integer(`Emergency Department Visits that Lead to Hospitalizations`*`PY Total`/1000)
  ) %>% rename(Benchmark_Dual=`Benchmark_Aged dual`,Benchmark_NonDual=`Benchmark_Aged non-dual`);

#' ## For the future, same table but without spaces in the names.
dat30a <- dat30;
names(dat30a) <- gsub('[ ,(\\)]','_',names(dat30a)) %>% 
  gsub('1_000','1000',.) %>% gsub('__','_',.) %>% gsub('_$|-','',.) %>% make.unique(sep='_');


dat40 <- left_join(dat30,dat30a,by='RQ',suffix=c('','##duplicate')) %>% select(!ends_with('##duplicate')) #%>% make.unique(sep='#');

#' ## Create the tall version 
dat50 <- dplyr::select(dat40,!matches('[^A-Za-z0-9_]')) %>% 
  tidyr::gather(key='QEXPU_Name',value='QEXPU_Value',-RQ);
dat50 <- subset(dat50,QEXPU_Name=='PY_Total') %>% dplyr::select(-'QEXPU_Name')  %>% 
  rename(PY=QEXPU_Value) %>% left_join(dat50,.) %>% arrange(RQ,QEXPU_Name) %>%
  mutate(compmatch=gsub('_Excluding_COVID','',QEXPU_Name)) %>%
  left_join(lookupcomponents,by = c(compmatch='QEXPU'))

#' ## Write out scripts to use in SQLServer

# sqlify ----

# These should get imported into Lookup_risknorms
write(sqlify2(risknormvals),file.path(.outputdir,'Lookup_RiskNormVals.sql'));


if(!exists('qexpu_cols')) qexpu_cols <- names(dat40);
write(sqlify2(dat40[,qexpu_cols]),file.path(.outputdir,'Lookup_QEXPU.sql'));

write(sqlify2(dat50),file.path(.outputdir,'Lookup_TallQEXPU.sql'));

#' 
NULL