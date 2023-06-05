
if(!require(data.table)){install.packages('data.table');require(data.table)}
if(!require(stringr)){install.packages('stringr');require(stringr)}
if(!require(tidyverse)){install.packages('tidyverse');require(tidyverse)}
if(!require(doParallel)){install.packages('doParallel');require(doParallel)}
if(!require(pdftools)){install.packages('pdftools');require(pdftools)}
if(!require(textclean)){install.packages('textclean');require(textclean)}

projects = readRDS('enepa_repository/metadata/eis_record_detail.rds')
documents = readRDS('enepa_repository/metadata/eis_document_record.rds')
pdf_files = list.files('enepa_repository/documents/',full.names = T,recursive = T)
txt_files = list.files('enepa_repository/text_as_datatable/',full.names = T,recursive = T)

still_need = documents[!gsub('pdf$','txt',documents$File_Name) %in% basename(txt_files),]

dr <- list.dirs('enepa_repository/documents')
dr2 <- gsub('documents','text_as_datatable',dr)
sapply(dr2[!dir.exists(dr2)],dir.create)
still_need <- still_need[order(-EIS.Number),]
for(i in 1:nrow(still_need)){
  pdf_name = grep(still_need$File_Name[i],pdf_files,value = T)
  print(pdf_name)
  text_name = gsub('enepa_repository/documents','enepa_repository/text_as_datatable',pdf_name,fixed = T)
  text_name <- gsub('pdf$','txt',text_name)  
  temp_text = tryCatch({pdftools::pdf_text(grep(still_need$File_Name[i],pdf_files,value = T))},error = function(e) NULL)
  if(!is.null(temp_text) & length(temp_text)>0 & any(temp_text!='')){
    temp_page = unlist(sapply(temp_text,function(x) x))
    temp_page = gsub('\\s{1,}',' ',temp_page)
    temp = data.table::data.table(Page = seq_along(temp_page),text = temp_page,stringsAsFactors = F)
    temp$text[nchar(temp$text)>10000] <- ''
    #temp = temp[!grepl('\\.{10,}',temp$text),]
    temp$text = textclean::replace_non_ascii(temp$text)
    #temp = temp[!grepl('^Figure [0-9]',temp$text),]
    temp$text = gsub('\\s{1,}$','',temp$text)
    #temp = temp[nchar(temp$text)>500,]
    temp = temp[!duplicated(text)&text!='',]
    if(nrow(temp)>0){
      fwrite(x = temp,file = text_name,sep = '\t')
    }
  }
}






# 
# #flist = list.files('../../../Desktop/text_as_datatable/',recursive = T,full.names = T)
# base.file = basename(flist)
# 
# eis_docs = documents
# eis_docs$FILE_LOC <- dirname(flist)[match(gsub('pdf$','txt',eis_docs$FILE_NAME),base.file)]
# 
# 
# #eis_docs[!file.exists(paste('../eis_documents/',eis_docs$FILE_LOC,eis_docs$FILE_NAME,sep = '/')),]
# have_pdf = eis_docs[file.exists(paste(eis_docs$FILE_LOC,gsub('pdf$','txt',eis_docs$FILE_NAME),sep = '/')),] 
# 
# nms = colnames(fread(paste(have_pdf[1]$FILE_LOC,gsub('pdf$','txt',have_pdf$FILE_NAME[1]),sep = '/')))
# 
# floc = 'enepa_repository/documents/'
# dir.create('enepa_repository/text_as_datatable')
# tfloc = 'enepa_repository/text_as_datatable/'
# docs = list.files(floc,recursive = T,full.names = T)
# text_docs = list.files(tfloc,recursive = T,full.names = T)
# 
# doc_record = fread('enepa_repository/meta_data/eis_document_record.csv')
# 
# mcores = detectCores() / 2
# doc_record = doc_record[PDF==T&!BAD_FILE,]
# doc_record$subd = str_extract(doc_record$File_Name,'^[0-9]{4}')
# 
# 
# text_names = paste0(tfloc,'/',doc_record$subd,'/',gsub('PDF$|pdf$','txt',doc_record$File_Name))
# 
# 
# flt = list.files('enepa_repository/text_as_datatable/',full.names = T,recursive = T)
# finfo = file.info(flt)
# 
# library(lubridate)
# old = month(ymd_hms(finfo$mtime))!=5
# 
# 
# pdf_list = list.files('enepa_repository/documents/',recursive = T,full.names = T,pattern = '^202[0-9]|^201[3-9]|extra')
# 
# 
# #pdf_list = pdf_list[grepl('^201[3-9]',basename(pdf_list))]
# pdf_list = gsub('_{2,}','_',pdf_list)
# 
# 
# text_list = list.files('enepa_repository/text_as_datatable/',recursive = T,full.names = T)
# text_list = text_list[grepl('^202[0-9]|^201[3-9]',basename(text_list))]
# 
# 
# #docs_unconverted = doc_record[!file.exists(text_names),]
# #docs_unconverted[EIS.Number=='201600457',]
# docs_unconverted <- pdf_list[!gsub('pdf$|PDF$','txt',basename(pdf_list)) %in% basename(text_list)]
# docs_unconverted = docs_unconverted[!grepl('([0-9]{8})_\\1\\.pdf$',basename(docs_unconverted))]
# #docs_unconverted = docs_unconverted[grepl('2020',basename(docs_unconverted))]
# 
# 
# 
# finfo = file.size(pdf_list)
# file.remove(pdf_list[finfo==0])
# 
# #docs_unconverted = doc_record[subd %in% 2013:2018,]
# #mclapply(which(!test_names %in% flt),function(i) {
# mclapply(1:length(docs_unconverted),function(i) { 
#   rm(id);rm(temp_text);rm(temp_page)
#   #i = which(documents$FILE_NAME=='41909_95853_FSPLT3_1658799.pdf')
#   fname = docs_unconverted[i]
#   
#   tname = paste0('enepa_repository/text_as_datatable/',str_extract(basename(fname),'^[0-9]{4}'),'/',basename(fname))
#   tname = gsub('pdf$|PDF$','txt',tname)
#     #if(file.exists(tname)){next}#temp = fread(tname);temp[,text:=NULL];all_page_dt = rbind(all_page_dt,temp,use.names=T,fill=T);next}
#   print(i)
#   temp_text = tryCatch({pdftools::pdf_text(fname)},error = function(e) NULL)
#   if(!is.null(temp_text) & length(temp_text)>0 & any(temp_text!='')){
#     temp_page = unlist(sapply(temp_text,function(x) x))
#     temp_page = gsub('\\s{1,}',' ',temp_page)
#     temp = data.table::data.table(Page = seq_along(temp_page),text = temp_page,stringsAsFactors = F)
#     temp$text[nchar(temp$text)>10000] <- ''
#     #temp = temp[!grepl('\\.{10,}',temp$text),]
#     temp$text = textclean::replace_non_ascii(temp$text)
#     #temp = temp[!grepl('^Figure [0-9]',temp$text),]
#     temp$text = gsub('\\s{1,}$','',temp$text)
#     #temp = temp[nchar(temp$text)>500,]
#     temp = temp[!duplicated(text)&text!='',]
#     #  if(file.exists(tname)&nrow(temp)==0){file.remove(tname)}
#     if(!file.exists(tname)|file.exists(tname)){fwrite(x = temp,file = tname,sep = '\t')}
#   }
# },mc.cores = mcores,mc.cleanup = T,mc.preschedule = T)
# 
#     


