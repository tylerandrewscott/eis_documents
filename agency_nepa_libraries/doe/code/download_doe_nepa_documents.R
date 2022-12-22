require(data.table)
require(pbapply)
require(RCurl)

library(tidyverse)
library(stringr)
library(rvest)
library(httr)
library(data.table)
bsite = 'https://www.energy.gov'
project_meta = 'doe_nepa_record.RDS'
document_meta = 'doe_nepa_document_record.RDS'
doe = readRDS(paste0('agency_nepa_libraries/doe/metadata/',project_meta))
#doe = doe[Project_Type=='EA'|NEPA_ID %in% c( "EIS-0436","EIS-0400"),]

#doe_docs = readRDS('agency_nepa_libraries/doe/metadata/doe_nepa_document_record.RDS')
save_loc = 'agency_nepa_libraries/doe/documents/'
doc_dt = readRDS(paste0('agency_nepa_libraries/doe/metadata/',document_meta))
doc_dt = doc_dt[NEPA_ID %in% doe$NEPA_ID,]
fnames = paste0(save_loc, doc_dt$YEAR,'/',paste(basename(doc_dt$NEPA_ID),basename(doc_dt$DOCUMENT_URL),sep = '--'))


bads = list.files(save_loc,recursive = T,pattern = 'txt$',full.names = T)
ex = file.exists(fnames)
go_get = which(!ex)

# zero_pages = pbsapply(fnames[ex],function(x) tryCatch({pdftools::pdf_info(x)$page==0}),cl = 8)
# file.remove(fnames[ex][as.vector(which(unlist(zero_pages)=='TRUE'))])
# bad_files = pbsapply(fnames[ex&grepl('^EA-',doc_dt$NEPA_ID)],function(x) tryCatch({pdftools::pdf_info(x)$page==0},error=function(e) NULL ))
# which_bad = which(as.vector(sapply(zero_pages,is.null)))
# file.remove(fnames[ex&grepl('^EA-',doc_dt$NEPA_ID)][which_bad])

for (i in go_get){
  url = doc_dt$DOCUMENT_URL[i]
  id <- basename(doc_dt$NEPA_ID[i])
  fname = paste0(save_loc, doc_dt$YEAR[i],'/',paste(id,basename(url),sep = '--'))
  dir = paste0(save_loc, doc_dt$YEAR[i])
  #if(file.exists(paste0(save_loc,paste(doe_docs$DOE_ID[i],basename(doe_docs$DOCUMENT_URL[i]))))){next}
  if(grepl('pdf$|PDF$',url)){
    if(!dir.exists(dir)){dir.create(dir)}
    #if(file.exists(fname)){next}
    if(!file.exists(fname)){
      print(i)
      res = tryCatch(httr::GET(url = url,write_disk(fname,overwrite=T)),error=function(e) return('bad download'))
      #test = tryCatch(pdftools::pdf_info(fname),error = function(e) NULL)
      tryPage = tryCatch(pdftools::pdf_info(fname),error = function(e) NULL)
      if(!is.null(tryPage)){
        if(tryPage$page==0){
        res = tryCatch(httr::GET(url,write_disk(fname,overwrite=T)),error=function(e) NULL)
      }
      } 
    }  
  }
}

library(pbapply)
library(lubridate)

fld = list.files('agency_nepa_libraries/doe/documents/',full.names = T,recursive = T)
info_list = pblapply(fld,function(x) tryCatch({pdf_info(x)},error = function(e) NULL),cl = 3)
temp = lapply(info_list,function(x) x$created)
temp[sapply(temp,is.null)]<-NA
created_date = do.call('c',temp)
dt = data.table(File = basename(fld),Created_Date=created_date)
fwrite(dt,file = 'agency_nepa_libraries/doe/metadata/pdf_created_date.csv')


