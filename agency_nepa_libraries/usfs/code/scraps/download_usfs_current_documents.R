library(tidyverse)
#doc_records = readRDS('output/all_doc_records.RDS')
#doc_records = doc_records[doc_records$Document_File!="javascript:void(0)",]

library(RCurl)

rec_files = list.files('input',"forest_service_document_",full.names = T)
fl = do.call(rbind,lapply(rec_files,file.info))
most_recent= rec_files[which(max(fl$mtime)==fl$mtime)]

fdf = read_csv(most_recent)
fdf$Document_Status = NA
fdf$Document_Status[fdf$Document_File=="javascript:void(0)"] <- 'No link'
fdf$Document_Status[file.exists(paste0('scratch/usfs_project_documents/',
    paste(fdf$Project_Num,gsub('\\/','',
        str_extract(fdf$Document_File,'(?:\\/[^\\/]{1,}$)')),sep='_')))] <- 'Have copy'

for(i in which(is.na(fdf$Document_Status))){
  print(i)
if(!url.exists(fdf$Document_File[i])){fdf$Document_Status[i]<-'Bad url'}
if(url.exists(fdf$Document_File[i])){
tryCatch(download.file(fdf$Document_File[i],
      destfile = paste0('scratch/usfs_project_documents/',
                        fdf$Project_Num[i],'_',gsub('.*\\/','',fdf$Document_File[i]))), error = function(e) print('bad try'))
  if(file.exists(paste0('scratch/usfs_project_documents/',
                        paste(fdf$Project_Num[i],gsub('\\/','',
                                                   str_extract(fdf$Document_File[i],'(?:\\/[^\\/]{1,}$)')),sep='_'))))
  {fdf$Document_Status[i]<-'Have copy'
}
}
}
write_csv(x = fdf,path=most_recent)






# 
# 
# install.packages('googledrive')
# library(googledrive)
# read_csv('../yampa/scratch/')
# library(tidyverse)
# eis = read_csv('../tuolumne/input/epa_master_repository/eis_record_detail.csv')
# forest_eis = eis[grepl('Forest|USFS|forest',eis$Agency),]
# 
# doc = read_csv('../tuolumne/input/epa_master_repository/eis_document_record.csv')
# forest_doc = doc[doc$EIS.Number %in% forest_eis$EIS.Number,]
# forest_doc = forest_doc[!is.na(forest_doc$File_Name),]
# write_csv(x = forest_doc,path = 'scratch/epa_forestservice_eis_document_records.csv')
# write_csv(x = forest_eis,path = 'scratch/epa_forestservice_eis_records.csv')
# library(pbapply)
# pblapply(seq_along(forest_doc$File_Name),function(x){
#   fpath = paste0('../tuolumne/scratch/eis_documents/',forest_doc$File_Name[x])
#   file.copy(fpath,to = paste0('scratch/epa_forestservice_eis_documents/',forest_doc$File_Name[x]))
# },cl = 10)
# 
# 
# file.copy()
# 
# 
# 
