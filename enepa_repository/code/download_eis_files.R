library(rvest)
library(stringr)
library(tidyverse)
library(httr)
library(data.table)
packs = c('rvest','stringr','tidyverse','httr','data.table','RCurl')
sapply(packs[!packs %in% installed.packages()[,'Package']],install.packages)
sapply(packs,require,character.only = T)

file_storage = 'enepa_repository/documents/'
record_df = fread('enepa_repository/meta_data/eis_record_detail.csv',stringsAsFactors = F)
record_df = record_df %>% mutate_if(is.logical,as.character)
record_df = data.table(record_df)
record_df = record_df[order(-EIS.Number)]

rerunALL = FALSE
if(!rerunALL){
docs = fread('enepa_repository/meta_data/eis_document_record.csv',stringsAsFactors = F)
current_flist = list.files(file_storage,recursive = T)
docs$YEAR = str_extract(docs$EIS.Number,'^[0-9]{4}')
docs = docs[YEAR>=2012,]
doc_df = docs}
if(rerunALL){
#needed_docs = docs[!paste(docs$YEAR,docs$File_Name,sep = '/') %in% current_flist,]
doc_df = data.table(EIS.Number = as.numeric(),Original_File_Name = as.character(),
                    File_Name =as.character(),BAD_FILE =  logical(),PDF = logical(),stringsAsFactors = F)
}

base_page = 'https://cdxapps.epa.gov'


finfo = file.info(paste0(file_storage,current_flist))
not_empty = finfo$size>0
if(any(!not_empty)){
file.remove(paste0(file_storage,current_flist[!not_empty]))
current_flist = current_flist[not_empty]
}

record_df$YEAR = str_extract(record_df$EIS.Number,'^[0-9]{4}')
#record_df = record_df[YEAR %in% 2013:2019,]

fls <- list.files('enepa_repository/documents/',recursive = T)
check_projs <- unique(str_extract(doc_df$File_Name[!doc_df$File_Name %in% basename(fls)],'^[0-9]{8}'))

record_df = record_df[({!EIS.Number %in% doc_df$EIS.Number} | EIS.Number %in% check_projs) & YEAR>2012,]

record_df[order(-EIS.Number),]


for (i in 1:nrow(record_df)){
  Sys.sleep(0.25)
  print(record_df$EIS.Number[i])
  try = RCurl::getURL(record_df$eis_url[i])
  if(!grepl('Server Error',try)){
  htmlNodes = record_df$eis_url[i] %>% read_html()  %>% html_nodes('a')
  file_url = grep('downloadAttachment',htmlNodes %>% html_attr('href') ,value=T)
  file_names = {htmlNodes %>% html_text()}[grepl('downloadAttachment',htmlNodes %>% html_attr('href'))]
  file_names = gsub('~|/','-',file_names)
  file_names = gsub('\\s{1,}','_',file_names)
  file_names = gsub('PDF$','pdf',file_names)
  if(length(file_url)==0){next}
  for (j in 1:length(file_url)){
    subd = str_extract(record_df$EIS.Number[i],'^[0-9]{4}')
    if(!dir.exists(paste0(file_storage,subd))){dir.create(paste0(file_storage,subd))}
    if(file.exists(paste0(file_storage,subd,'/',paste(record_df$EIS.Number[i],file_names[j],sep='_')))){
        tdf = data.frame(EIS.Number = record_df$EIS.Number[i],Original_File_Name = file_names[j],File_Name = paste(record_df$EIS.Number[i],file_names[j],sep='_'),BAD_FILE=F,PDF = grepl('PDF$',toupper(file_names[j])),stringsAsFactors = F)
    }
    if(!file.exists(paste0(file_storage,subd,'/',paste(record_df$EIS.Number[i],file_names[j],sep='_')))){
        temp_name = paste0(file_storage,subd,'/',paste(record_df$EIS.Number[i],file_names[j],sep='_'))
        temp_name <- gsub('PDF$','pdf',temp_name)
        download = tryCatch(httr::GET(paste0(base_page,file_url[j]), verbose(),write_disk(temp_name), overwrite=TRUE),error=function(e) NULL)
        temp_info = file.info(temp_name)
      if(is.null(download)){
          tdf = data.frame(EIS.Number = record_df$EIS.Number[i],Original_File_Name = file_names[j],File_Name = paste(record_df$EIS.Number[i],file_names[j],sep='_'),BAD_FILE=T,PDF = grepl('PDF$',toupper(file_names[j])),stringsAsFactors = F)
      }
      if(!is.null(download)){
          temp_info$size = file.info(temp_name)
        if(file.size(temp_name)==0&all(duplicated(file_names))){
          
          tdf = data.frame(EIS.Number = record_df$EIS.Number[i],Original_File_Name = file_names[j],File_Name = paste(record_df$EIS.Number[i],file_names[j],sep='_'),BAD_FILE=T,PDF = grepl('PDF$',toupper(file_names[j])),stringsAsFactors = F)
        }
        if(file.size(temp_name)==0&file_names[j] %in% file_names[-j]){
          file.remove(temp_name)
            next          
          }
        if(file.size(temp_name)>0){
          tdf = data.frame(EIS.Number = record_df$EIS.Number[i],Original_File_Name = file_names[j],File_Name = paste(record_df$EIS.Number[i],file_names[j],sep='_'),BAD_FILE=F,PDF = grepl('PDF$',toupper(file_names[j])),stringsAsFactors = F)
        } 
      }
    }
    doc_df = rbind(doc_df,tdf,use.names = T,fill = T)
  }

  }
}

write.csv(x = doc_df,file = paste0('enepa_repository/meta_data/eis_document_record','.csv'),row.names = F)


# 
# 
