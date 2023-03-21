

td = tempdir()
pack = c('data.table','pbapply','stringr','tidyverse','doParallel','pdftools','tesseract')
need = pack[!pack %in% rownames(installed.packages())]
if(length(need)>0){install.packages(need)}
sapply(pack,require,character.only = T)

floc = 'agency_nepa_libraries/blm/nepa_documents/'
dir.create('agency_nepa_libraries/blm/text_as_datatable/')
tfloc = 'agency_nepa_libraries/blm/text_as_datatable/'

docs = list.files(floc,recursive = T,full.names = T)
mcores = detectCores() / 2
suffix = str_remove_all(docs,'.+\\.')
docs = docs[suffix %in% c('pdf','docx','doc')]

ids = str_remove(basename(docs),'--.+')
dirs = dirname(docs)
yeardir = basename(dirs)
sapply(unique(dirs),dir.create)

tnames = gsub('\\.pdf$|\\.doc$|\\.docx$','.txt',docs)

text_version = paste(tfloc,yeardir,basename(tnames),sep = '/')


transline_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQck262IIZxf_o5q2Ci9gRlY2qTPzw0WqCsEch3RXNAqYVd-JKZgZLpSPGc3GxB3TsW0Dek2hd3_H6k/pub?gid=811733457&single=true&output=csv'

projs = fread(transline_url)
projs = projs[Agency%in%c('DOE','BLM')]

need_to_convert = which(!file.exists(text_version)&!grepl('CX|OTHER_NEPA|DNA',tnames)&ids %in% projs$ID)

# text_list = list.files('agency_nepa_libraries/blm/text_as_datatable/',full.names = T,recursive = T)
# reset = text_list[grepl('\\.pdf\\.txt$',text_list)]
# fixes = gsub('\\.pdf\\.txt$','.txt',reset)
# file.rename(reset,fixes)

#mclapply(which(!test_names %in% flt),function(i) {
pblapply(rev(need_to_convert),function(i) {
  #i = which(documents$FILE_NAME=='41909_95853_FSPLT3_1658799.pdf')
  tname = text_version[i]
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
        ocr_text = ocr(fname)
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
  },cl = 4)


#parallel::stopCluster(cl)
#stopImplicitCluster()




