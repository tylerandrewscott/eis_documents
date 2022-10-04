
if(!require(data.table)){install.packages('data.table');require(data.table)}
if(!require(stringr)){install.packages('stringr');require(stringr)}
if(!require(tidyverse)){install.packages('tidyverse');require(tidyverse)}
if(!require(doParallel)){install.packages('doParallel');require(doParallel)}
if(!require(pdftools)){install.packages('pdftools');require(pdftools)}

floc = 'agency_nepa_libraries/usfs/documents/'
dir.create('agency_nepa_libraries/usfs/text_as_datatable/')
tfloc = 'agency_nepa_libraries/usfs/text_as_datatable/'
docs = list.files(floc,recursive = T,pattern = 'pdf$|PDF$')
dirs = dirname(docs)
base_doc = basename(docs)
ids = str_extract(base_doc,'^[0-9]{1,}')

mcores = detectCores() / 2
fnames = paste0(floc,docs)

sapply(paste0('agency_nepa_libraries/usfs/text_as_datatable/',unique(dirs)),dir.create)
tnames = paste(tfloc,dirs,paste(ids,str_replace(base_doc,'\\.PDF$|\\.pdf$','.txt'),sep = '--'),sep = '/')

tiny = sapply(tnames,function(x) file.info(x)$size)
exists = sapply(tnames,file.exists)
file.remove(tnames[!is.na(tiny) & tiny==10])
need_to_convert = which(!file.exists(tnames))

#mclapply(which(!test_names %in% flt),function(i) {
mclapply(need_to_convert,function(i) {
  #i = which(documents$FILE_NAME=='41909_95853_FSPLT3_1658799.pdf')
  tname = tnames[i]
  if(file.exists(fnames[i])){
    #if(file.exists(tname)){next}#temp = fread(tname);temp[,text:=NULL];all_page_dt = rbind(all_page_dt,temp,use.names=T,fill=T);next}
    rm(id);rm(temp_text);rm(temp_page)
    temp_text = tryCatch({pdftools::pdf_text(fnames[i])},error = function(e) NULL)
    if(!is.null(temp_text) & length(temp_text)>0 & !all(temp_text=='')){
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
      
      if(!file.exists(tname)&nrow(temp)!=0){fwrite(x = temp,file = tname,sep = '\t')}
    }
  }
},mc.cores = mcores,mc.cleanup = T,mc.preschedule = T)


#parallel::stopCluster(cl)
#stopImplicitCluster()




