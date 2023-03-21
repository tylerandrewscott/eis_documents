
library(rvest)
library(data.table)
library(httr)
library(stringr)
library(xml2)
library(pbapply)
library(lubridate)

storage = 'agency_nepa_libraries/blm/nepa_documents/'
precord = 'agency_nepa_libraries/blm/metadata/new_project_record_detailed.rds'
drecord = 'agency_nepa_libraries/blm/metadata/new_document_record.csv'
docs = fread(drecord)
docs = docs[!duplicated(docs),]
proj = readRDS(precord)

docs$NEPA_ID = proj$nepaNumber[match(docs$pnum,proj$projectId)]

docs$Year = str_extract(gsub('^[A-Z0-9]{3,}-','',gsub('DOI-BLM-([A-Za-z0-9]|\\s)+-','',docs$NEPA_ID)),'^[0-9]{4}')
flist = list.files('agency_nepa_libraries/blm/nepa_documents/',recursive = T)


finfo = file.info(paste0('agency_nepa_libraries/blm/nepa_documents/',flist))
file.remove(paste0('agency_nepa_libraries/blm/nepa_documents/',flist[finfo$size<=970]))


base = 'https://eplanning.blm.gov/'

#table(file.exists(paste0(storage,docs$Year,'/',docs$NEPA_ID,'_',gsub('\\s','_',basename(docs$File_Name)))))
# 
# f1 = paste0(storage,docs$Year,'/',docs$NEPA_ID,'--',gsub('\\s{1,}','_',basename(docs$File_Name)))
# t1 = file.exists(f1)
# f2 = paste0(storage,docs$Year,'/',docs$NEPA_ID,'_',gsub('\\s{1,}','_',basename(docs$File_Name)))
# t2 = file.exists(f2)
# 
# for(i in seq_along(f1)){
#   if(!t1[i]&t2[i]){file.rename(from = f2[i],to = f1[i])}
# 

fnames = paste0(storage,docs$Year,'/',docs$NEPA_ID,'--',gsub('\\s{1,}','_',basename(docs$documentRendition.uri)))
ex = file.exists(fnames)

doc = grepl('doc$|docx$',fnames)

downloads = pblapply(rev(which(!ex)),function(x) {
    if(!dir.exists(paste0(storage,docs$Year[x]))){dir.create(paste0(storage,docs$Year[x]))}
    tryCatch(httr::GET(paste0(base,docs$documentRendition.uri[x]), verbose(),write_disk(fnames[x]), overwrite=TRUE),error=function(e) NULL)
  },cl = 5)
