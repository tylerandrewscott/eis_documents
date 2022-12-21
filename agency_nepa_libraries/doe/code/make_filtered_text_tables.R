
if(!require(data.table)){install.packages('data.table');require(data.table)}
if(!require(stringr)){install.packages('stringr');require(stringr)}
if(!require(tidyverse)){install.packages('tidyverse');require(tidyverse)}
if(!require(doParallel)){install.packages('doParallel');require(doParallel)}
if(!require(pdftools)){install.packages('pdftools');require(pdftools)}
if(!require(pbapply)){install.packages('pbapply');require(pdftools)}



floc = 'agency_nepa_libraries/doe/documents/'
tfloc = 'agency_nepa_libraries/doe/text_as_datatable/'
docs = list.files(floc,recursive = T,full.names = T)
mcores = detectCores() / 2
#fnames = paste0(floc,docs)
ids = str_remove(basename(docs),'--.*')

base_floc = 'agency_nepa_libraries/doe/text_as_datatable/'

dirs = dirname(gsub(floc,'',docs,fixed = T))
sapply(paste0(base_floc,unique(dirs)),dir.create)
tnames = gsub('\\.pdf$|\\.PDF$','.txt',docs)
tnames = gsub(floc,tfloc,tnames,fixed = T)


transline_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQck262IIZxf_o5q2Ci9gRlY2qTPzw0WqCsEch3RXNAqYVd-JKZgZLpSPGc3GxB3TsW0Dek2hd3_H6k/pub?gid=811733457&single=true&output=csv'

projs = fread(transline_url)
projs = projs[Agency%in%c('DOE','BLM')]



need_to_convert =  which(!file.exists(tnames)&ids %in% projs$ID)
#need_to_convert = which(!file.exists(tnames)&grepl('FEA|FEIS|EIS',tnames))

docs[need_to_convert]

#mclapply(which(!test_names %in% flt),function(i) {
pblapply(rev(need_to_convert),function(i) {
  #i = which(documents$FILE_NAME=='41909_95853_FSPLT3_1658799.pdf')
  tname = tnames[i]
  fname = docs[i]
  if(file.exists(fname)){
    #if(file.exists(tname)){next}#temp = fread(tname);temp[,text:=NULL];all_page_dt = rbind(all_page_dt,temp,use.names=T,fill=T);next}
    rm(id);rm(temp_text);rm(temp_page)
    print(i)

    temp_text = tryCatch({pdftools::pdf_text(fname)},error = function(e) NULL)
    #if(!is.null(temp_text)&all(temp_text=='')){fwrite(x = data.table::data.table(Page = numeric(),text = character(),stringsAsFactors = F),file = tname,sep = '\t')}
    if(length(temp_text)>0){
      temp_page = unlist(sapply(temp_text,function(x) x))
      temp_page = gsub('\\s{1,}',' ',temp_page)
      temp = data.table::data.table(Page = seq_along(temp_page),text = temp_page,stringsAsFactors = F)
      temp = temp[nchar(temp$text)<10000,]
      temp = temp[!grepl('\\.{10,}',temp$text),]
      temp$text = textclean::replace_non_ascii(temp$text)
      temp = temp[!grepl('^Figure [0-9]',temp$text),]
      temp$text = gsub('\\s{1,}$','',temp$text)
      temp = temp[nchar(temp$text)>500,]
      temp = temp[!duplicated(text),]
      #   if(nrow(temp)==0){
      #  print(fname)
      #  next
      #  }
      if(!file.exists(tname)&nrow(temp)>0){fwrite(x = temp,file = tname,sep = '\t')}
    }
    
    if(!file.exists(tname)&(nrow(temp)==0|length(temp_text)==0)){
      ocr_text = tesseract::ocr(fname)
      temp_page = unlist(sapply(ocr_text,function(x) x))
      temp_page = gsub('\\s{1,}',' ',temp_page)
      temp = data.table::data.table(Page = seq_along(temp_page),text = temp_page,stringsAsFactors = F)
      temp = temp[nchar(temp$text)<10000,]
      temp = temp[!grepl('\\.{10,}',temp$text),]
      temp$text = textclean::replace_non_ascii(temp$text)
      temp = temp[!grepl('^Figure [0-9]',temp$text),]
      temp$text = gsub('\\s{1,}$','',temp$text)
      temp = temp[nchar(temp$text)>500,]
      temp = temp[!duplicated(text),]
      if(!file.exists(tname)&nrow(temp)>0){fwrite(x = temp,file = tname,sep = '\t')}
      file.remove(list.files(pattern = 'png$'))
      rm(ocr_text);rm(temp_page);rm(temp)
    }
  }
},cl = 2)



# 
# #mclapply(which(!test_names %in% flt),function(i) {
# mclapply(need_to_convert,function(i) {
#   #i = which(documents$FILE_NAME=='41909_95853_FSPLT3_1658799.pdf')
#   tname = tnames[i]
#   if(file.exists(fnames[i])){
#   #if(file.exists(tname)){next}#temp = fread(tname);temp[,text:=NULL];all_page_dt = rbind(all_page_dt,temp,use.names=T,fill=T);next}
#   rm(id);rm(temp_text);rm(temp_page)
#   print(i)
#   temp_text = tryCatch({pdftools::pdf_text(fnames[i])},error = function(e) NULL)
#   if(length(temp_text)>0){
#     temp_page = unlist(sapply(temp_text,function(x) x))
#     temp_page = gsub('\\s{1,}',' ',temp_page)
#     temp = data.table::data.table(Page = seq_along(temp_page),text = temp_page,stringsAsFactors = F)
#     temp = temp[nchar(temp$text)<10000,]
#     temp = temp[!grepl('\\.{10,}',temp$text),]
#     temp$text = textclean::replace_non_ascii(temp$text)
#     temp = temp[!grepl('^Figure [0-9]',temp$text),]
#     temp$text = gsub('\\s{1,}$','',temp$text)
#     temp = temp[nchar(temp$text)>500,]
#     temp = temp[!duplicated(text),]
#     if(file.exists(tname)&nrow(temp)==0){file.remove(tname)}
#     if(!file.exists(tname)&nrow(temp)>0){fwrite(x = temp,file = tname,sep = '\t')}
#   }
# }}
# ,mc.cores = mcores,mc.cleanup = T,mc.preschedule = T)


#parallel::stopCluster(cl)
#stopImplicitCluster()




