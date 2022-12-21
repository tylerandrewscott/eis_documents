require(data.table)
require(pbapply)
require(RCurl)
#require(boxr)
library(tidyverse)
library(stringr)
library(rvest)
library(httr)
library(data.table)
#all or not yet seen
all <- F
bsite = 'https://www.energy.gov'
project_meta = 'agency_nepa_libraries/doe/metadata/doe_nepa_record.RDS'
document_meta = 'agency_nepa_libraries/doe/metadata/doe_nepa_document_record.RDS'
doe = readRDS(project_meta)
doe$URL <- gsub('enery\\.gov','energy.gov',doe$URL)
doc_meta <- readRDS(document_meta)
if(!all){doe <- doe[!doe$NEPA_ID %in% doc_meta$NEPA_ID,]}

tmp_doc_lists = pblapply(1:nrow(doe),function(i) {
  if(!grepl('[a-z]',doe$URL[i])){turl <-paste0('https://energy.gov/node/',doe$URL[i])}else{turl = doe$URL[i]}
  if(!httr::http_error(turl)){
    print(turl)
    temp_session = session(turl)
    doe$full_url[i] = temp_session$url
    temp_html = temp_session %>% read_html()
    #doe$project_description[i] <-  temp_html %>% html_nodes('p') %>% html_text(trim=T) %>% .[.!=''] %>% paste(.,collapse= ' ')
    temp_docs = temp_html %>% html_nodes('.listing-item-link')
    if(length(temp_docs)>0){
      temp_doc_dt = data.table(NEPA_ID = doe$NEPA_ID[i],DOCUMENT_TITLE =  temp_docs %>% html_text(trim=T),
                               DOCUMENT_URL = temp_docs %>% html_attr('href'),FILE_NAME = NA)
      #if(!all(temp_doc_dt$DOCUMENT_URL %in% doc_dt$DOCUMENT_URL)){
      temp_doc_dt
      #}
    }
  }
},cl = 1)


doc_dt = rbindlist(tmp_doc_lists,use.names = T,fill = T)
doc_dt = doc_dt[!duplicated(doc_dt)]

nonpdf = which(!grepl('pdf$|PDF$',doc_dt$DOCUMENT_URL))

doc_sites = pblapply(nonpdf,function(i) {
  hrefs = read_html(paste0(bsite,doc_dt$DOCUMENT_URL[i])) %>% html_nodes('.file a')
  hrefs = hrefs[grepl('pdf$',hrefs %>% html_attr('href'))]
  fls = hrefs %>% html_attr('href')
  hrefs2 = read_html(paste0(bsite,doc_dt$DOCUMENT_URL[i])) %>% html_nodes('#block-system-main a')  
  if(length(hrefs2)==1){
  base_sub = str_extract(hrefs2 %>% html_attr('href'),'.+\\.gov')
  other_files = grep('pdf$',read_html(hrefs2 %>% html_attr('href')) %>% html_nodes('a') %>% html_attr('href'),value = T)
  fls = c(fls, paste0(base_sub,other_files))
  }
  if(length(fls)>0){
    temp_doc_dt = data.table(NEPA_ID = doc_dt$NEPA_ID[i],DOCUMENT_TITLE =  hrefs %>% html_text(trim=T),
                             DOCUMENT_URL = fls,FILE_NAME = NA)
    temp_doc_dt}
},cl = 3)


more_docs = rbindlist(doc_sites[!sapply(doc_sites,function(x) any(class(x)%in%'try-error'))],use.names=T,fill=T)

doc_dt = rbind(doc_dt,more_docs,use.names=T,fill=T) 

doc_dt = doc_dt[grepl('pdf$|PDF$',DOCUMENT_URL),]  

#doe_docs = readRDS('agency_nepa_libraries/doe/metadata/doe_nepa_document_record.RDS')
save_loc = 'agency_nepa_libraries/doe/documents/'
doc_dt$DOCUMENT_URL = ifelse(grepl('^\\/',doc_dt$DOCUMENT_URL),paste0(bsite,doc_dt$DOCUMENT_URL),doc_dt$DOCUMENT_URL)
doc_dt$YEAR = doe$YEAR[match(doc_dt$NEPA_ID,doe$NEPA_ID)]
doc_dt$FILE_NAME = basename(doc_dt$DOCUMENT_URL)
doc_dt = doc_dt[!duplicated(DOCUMENT_URL),]

doc_dt <- doc_dt[!FILE_NAME%in%doc_meta$FILE_NAME,]
doc_meta <- rbind(doc_meta,doc_dt,use.names = T,fill = T)
saveRDS(doc_meta,file = document_meta)

