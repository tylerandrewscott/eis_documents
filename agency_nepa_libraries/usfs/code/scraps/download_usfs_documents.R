library(tidyverse)
#doc_records = readRDS('output/all_doc_records.RDS')
#doc_records = doc_records[doc_records$Document_File!="javascript:void(0)",]
library(data.table)
library(RCurl)

proj_flist = list.files('input',pattern = 'forest_service_project_overview',full.names = T)
proj_finfo = file.info(proj_flist)
proj_most_recent= proj_flist[which.max(proj_finfo$mtime)]
proj_fdf = fread(proj_most_recent)
proj_fdf$forest = str_extract(gsub('https://www.fs.usda.gov/wps/portal/fsinternet/cs/projects/','',
                                   gsub('https://www.fs.usda.gov/projects/','',proj_fdf$NF,fixed=T),fixed=T),'[^\\/]{1,}')
proj_fdf$Project_Num = str_extract(proj_fdf$Page,'[0-9]{1,}$')

flist = list.files('input/',pattern = 'forest_service_document',full.names = T)
finfo = file.info(flist)
most_recent= flist[which.max(finfo$mtime)]
fdf = fread(most_recent)
fdf$forest = proj_fdf$forest[match(fdf$Project_Num,proj_fdf$Project_Num)]


fdf$Document_Status = NA
fdf$Document_Status[fdf$Document_File=="javascript:void(0)"] <- 'No link'

doc_loc = '../../../../net/tmp/tscott1/manitou_scratch/scratch/usfs_project_documents'
doc_files = list.files(doc_loc,recursive = T)
fdf$Document_Status[paste(fdf$Project_Num,basename(fdf$Document_File),sep='_') %in% basename(doc_files)] <- 'Have copy'

library(curl)
library(RCurl)
dir_forests = paste(doc_loc,unique(fdf$forest),sep='/')
sapply(dir_forests[!dir.exists(dir_forests)],dir.create)


for(i in which(is.na(fdf$Document_Status))){
  print(i)
if(!url.exists(fdf$Document_File[i])){fdf$Document_Status[i]<-'Bad url'}
if(url.exists(fdf$Document_File[i])){
tryCatch(download.file(fdf$Document_File[i],
      destfile = paste0(doc_loc,'/',fdf$forest[i],'/',
                        fdf$Project_Num[i],'_',gsub('.*\\/','',fdf$Document_File[i]))), error = function(e) print('bad try'))
  if(file.exists(paste0(doc_loc,'/',fdf$forest[i],'/',
                        paste(fdf$Project_Num[i],gsub('\\/','',
                                                   str_extract(fdf$Document_File[i],'(?:\\/[^\\/]{1,}$)')),sep='_'))))
  {fdf$Document_Status[i]<-'Have copy'
}
}
}
fwrite(x = fdf,file = most_recent)


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
