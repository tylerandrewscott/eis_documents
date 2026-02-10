library(rvest)
library(stringr)
library(tidyverse)
library(httr)
library(data.table)
###  CURRENTLY NO UPDATE FUNCTIONALITY, NEED TO ADD ####
## if false, starts from beginning ####
update = T
#### 
packs = c('rvest','stringr','tidyverse','httr','data.table','RCurl')
sapply(packs[!packs %in% installed.packages()[,'Package']],install.packages)
sapply(packs,require,character.only = T)

file_storage = 'enepa_repository/box_files/documents/'
doc_file_name <- 'enepa_repository/metadata/eis_document_record.rds'
record_df = readRDS('enepa_repository/metadata/eis_record_detail.rds')

record_df = record_df %>% mutate_if(is.logical,as.character)
record_df = data.table(record_df)
record_df = record_df[order(-EIS.Number)]

record_df$YEAR <- str_extract(record_df$EIS.Number,'^[0-9]{4}')
record_df <- record_df[YEAR>=2012,]

current_flist = list.files(file_storage,recursive = T,pattern = 'pdf')

docs = readRDS(doc_file_name)#fread('enepa_repository/metadata/eis_document_record.csv',stringsAsFactors = F)
docs$YEAR = str_extract(docs$EIS.Number,'^[0-9]{4}')
#docs = docs[YEAR>=2012,]
doc_df = docs
doc_df <- doc_df[doc_df$EIS.Number %in% record_df$EIS.Number,]

base_page = 'https://cdxapps.epa.gov'
finfo = file.info(paste0(file_storage,current_flist))
not_empty = finfo$size>0

#record_df = record_df[YEAR %in% 2013:2019,]
library(pbapply)
fls <- list.files('enepa_repository/box_files/documents/',recursive = T)



which_are_epa <- pblapply(1:nrow(record_df),function(i){
  #print(record_df$EIS.Number[i])
  id <- record_df$EIS.Number[i]
  url <- record_df$eis_url[i]
  try = RCurl::getURL(record_df$eis_url[i])
  lines <- readLines(record_df$eis_url[i])
  comm <- grep('Comment\\sLetter\\(s\\)',lines)
  if(identical(comm,integer(0))){return(data.table(EIS.Number=id,File.Name = NA))}else{
  lines <- lines[comm:{comm+20}]
  links <- lines[grepl('href',lines)]
  file_names <- str_extract(links,'(?:\\>).+(?=\\<)')
  file_names <- str_remove(file_names,'^\\>')
  full_name <- paste0(id,'_',file_names)
  return(data.table(EIS.Number=id,File.Name = full_name))}
  },cl = 4)

epa_comments <- rbindlist(which_are_epa,use.names = T,fill = T)
epa_comments$File.Name = gsub('~|/','-',epa_comments$File.Name)
epa_comments$File.Name = gsub('\\s{1,}','_',epa_comments$File.Name)
epa_comments$File.Name = gsub('PDF$','pdf',epa_comments$File.Name)
epa_comments$File.Name = gsub('pdf\\.pdf$','.pdf',epa_comments$File.Name)
epa_comments$File.Name <- gsub('PDF$','pdf',epa_comments$File.Name)
epa_comments$File.Name <- str_remove_all(epa_comments$File.Name,'\\(|\\)|\\&|\\,')

epas <- doc_df[doc_df$File_Name %in% epa_comments$File.Name,]
saveRDS(epas,file = 'enepa_repository/metadata/epa_comment_record.RDS')


epas <- readRDS('enepa_repository/metadata/epa_comment_record.RDS')
tx <- list.files('enepa_repository/box_files/text_as_datatable/',recursive = T)

library(pdftools)
pd <- list.files('enepa_repository/box_files/documents/',recursive = T)
letters <- pd[basename(pd) %in% epas$File_Name]
storage_loc <- 'enepa_repository/box_files/documents/'

library(imagefx)
library(jpeg)

have <- list.files('enepa_repository/box_files/epa_comment_letters/')

for(p in letters){
  print(p)
  full_name <- paste0(storage_loc,p)
  new_name <- str_replace(basename(full_name),'pdf$','txt')
  new_con <- paste0('enepa_repository/box_files/epa_comment_letters/',new_name)
  if(!file.exists(new_con)){
    oc <- tryCatch(pdf_ocr_text(full_name),error = function(e) NULL)
    if(!is.null(oc)){writeLines(text = oc,con = new_con)}
  }
}
  
  
