library(data.table)
flist = list.files('enepa_repository/meta_data/','eis_document_rec')
stor = 'enepa_repository/meta_data/'
#flist = flist[flist!='eis_document_record.csv']
empty = data.table(stringsAsFactors = F)
fils = for(f in flist){
  print(f)
  temp = fread(paste0(stor,f))
  empty = rbindlist(list(empty,temp))
  empty = empty[!duplicated(empty)]
  }

fwrite(empty,paste0(stor,'eis_document_record.csv'))


for(f in flist[flist!='eis_document_record.csv']){
  file.remove(paste0(stor,f))}

dim(empty)
test = rbindlist(fils)
dim(test)
table(duplicated(test))
lapply(flist,function(x) file.exists(paste0(stor,x)))
