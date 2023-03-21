
library(rvest)
library(data.table)
library(httr)
library(stringr)
library(xml2)
library(pbapply)
library(lubridate)

storage = 'agency_nepa_libraries/blm/nepa_documents/'
drecord = 'agency_nepa_libraries/blm/metadata/document_lup_record.csv'
docs = fread(drecord)
docs = docs[!duplicated(docs),]
docs$Year <- str_extract(gsub('^[A-Z0-9]{3,}-','',gsub('DOI-BLM-([A-Za-z0-9]|\\s)+-','',docs$NEPA_ID)),'^[0-9]{4}')
docs = docs[!grepl('CX|DNA|OTHER',NEPA_ID),]
flist = list.files('agency_nepa_libraries/blm/nepa_documents/',recursive = T)
base = 'https://eplanning.blm.gov/'
goget = which(!file.exists(paste0(storage,docs$Year,'/',docs$NEPA_ID,'--',gsub('\\s','_',basename(docs$File_Name))))&grepl('EIS|EA',docs$NEPA_ID))

downloads = pblapply(goget,function(x) {
  if(!file.exists(paste0(storage,docs$Year[x],'/',docs$NEPA_ID[x],'--',gsub('\\s','_',basename(docs$File_Name[x]))))){
    if(!dir.exists(paste0(storage,docs$Year[x]))){dir.create(paste0(storage,docs$Year[x]))}
    tryCatch(httr::GET(paste0(base,docs$url[x]), verbose(),write_disk(paste0(storage,docs$Year[x],'/',paste(docs$NEPA_ID[x],basename(docs$url[x]),sep='--'))), overwrite=TRUE),error=function(e) NULL)
    }
},cl = 5)
