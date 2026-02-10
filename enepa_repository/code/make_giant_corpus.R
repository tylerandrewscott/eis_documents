
packs = c('data.table','stringr','tidyverse','doParallel','pdftools','pbapply')

have = sapply(packs,require,character.only=T)

lapply(packs[!have],function(p) {install.packages(p);require(p,character.only = T)})

flist = list.files('enepa_repository/box_files/text_as_datatable/',full.names = T,recursive = T)
flist_year = str_extract(basename(flist),'^[0-9]{4}')

big_eis_text = data.table(Page = numeric(),text = character(),File = character())

years = 2012:year(Sys.Date())


corpus_year_files = paste0('enepa_repository/corpus/eis_corpus_',years,'.txt')
for(corpus_name in corpus_year_files){
  if(!file.exists(corpus_name)){fwrite(x = big_eis_text,file = corpus_name,sep = '\t')}
}


for(corp in corpus_year_files){
  temp = fread(corp,sep = '\t')
  yr = str_extract(corp,'[0-9]{4}')
  temp_flist = flist[flist_year==yr]
  temp_flist = temp_flist[!basename(temp_flist) %in% temp$File]
  if(length(temp_flist)>0){
  ### this batches in small groups to avoid Box streaming lag issues
  tiles = dplyr::ntile(1:length(temp_flist),max(floor(length(temp_flist)/10),1))
  uq_tiles = unique(tiles)
  for(u in uq_tiles){
    print(u)
    temp_fname_list = temp_flist[uq_tiles==u]
    combined_files <- rbindlist(pbapply::pblapply(temp_fname_list,function(x) {xx<-fread(x,sep = '\t',);xx$File = basename(x);xx}))
    fwrite(x = combined_files,file = corp,append = T,verbose = F,sep = '\t')
    rm(combined_files)
  }
  }
Sys.sleep(5)
}






