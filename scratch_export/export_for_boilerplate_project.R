pack = c('data.table','zip','stringr','pbapply')
need = pack[!pack %in% installed.packages()[,'Package']]
lapply(need,install.packages)
lapply(pack,require,character.only=T)

corpus_years = 2013:2020
corp = list.files('enepa_repository/corpus/',full.names = T,recursive = T)
keep_corp = corp[str_extract(corp,'[0-9]{4}') %in% corpus_years ]

epa = fread('enepa_repository/meta_data/eis_record_detail.csv')
epa = epa[Document=='Final'&str_extract(EIS.Number,'^[0-9]{4}') %in% corpus_years,]

docs = fread('enepa_repository/meta_data/eis_document_record.csv')
docs2 = fread("enepa_repository/meta_data/extra_docs.csv")
docs = rbindlist(list(docs,docs2),use.names = T,fill = T)
docs = docs[EIS.Number %in% epa$EIS.Number,]

all_corp = lapply(keep_corp,fread,sep = '\t')
corp_dt = rbindlist(all_corp)
eis_ids = str_extract(corp_dt$File,'^[0-9]{1,}')
corp_dt = corp_dt[eis_ids %in% epa$EIS.Number,]

saveRDS(object = corp_dt, file = '../tuolumne/boilerplate_project/input/feis_corpus_2013-2020.rds')

fwrite(docs,'../tuolumne/boilerplate_project/input/feis_document_record.csv')
fwrite(epa,'../tuolumne/boilerplate_project/input/feis_record_detail.csv')

good_string = 'PREPARERS|CONTRIBUTORS|Preparers|Contributors|Consultants|Interdisciplinary Team'#LIST OF PREPARERS|List of Preparers|List Of Preparers|PREPARERS AND CONTRIBUTORS|Preparers and Contributors|Preparers \\& Contributors|Preparers And Contributors'
bad_string = 'TABLE OF CONTENTS|Table of Contents|^CONTENTS|Contents|How to Use This Document|Date Received'

tfiles = list.files('enepa_repository/text_as_datatable/',full.names = T,recursive = T)
split_by_proj = split(tfiles,str_extract(basename(tfiles),'^[0-9]{8}'))

split_by_proj <- split_by_proj[names(split_by_proj) %in% epa$EIS.Number]

pages_list = pblapply(split_by_proj,function(i) {
  if(any(grepl(good_string,i))){
    special_sections = grep(good_string,i,value = T)
    temp_txt <- rbindlist(lapply(special_sections,function(j)  {tt = fread(j);tt$FILE <- basename(j);tt}),fill = T,use.names = T)
    temp_txt$keep = temp_txt$keep2 = 1
  }
  if(!any(grepl(good_string,i))){
    base_file = basename(i)
    id = str_remove(basename(base_file),'_.*')
    #start_dt = data.table(FILE_NAME = base_file,PROJECT_ID = id,people = list(),orgs = list())
    temp_txt =  rbindlist(lapply(i,function(j)  {j_temp = fread(j);j_temp$FILE = basename(j);j_temp}),fill = T,use.names = T)
    temp_txt$keep = temp_txt$keep2 = 0
    temp_txt$looks_like_frontmatter = !grepl('[A-Z0-9]',str_remove(temp_txt$text,'.*\\s'))
    #  temp_txt = temp_txt[grepl('[A-Z0-9]',str_remove(temp_txt$text,'.*\\s')),]
    prep_papers = grepl(good_string,str_sub(temp_txt$text,1,200))&!grepl(bad_string,str_sub(temp_txt$text,1,200))
    temp_txt$keep[prep_papers] <- 1
    prep_papers2 = grepl(good_string,  str_sub(temp_txt$text,nchar(temp_txt$text)-200,nchar(temp_txt$text)))&!grepl(bad_string,  str_sub(temp_txt$text,1,200))
    temp_txt$keep2[prep_papers2] <- 1
    temp_txt = temp_txt[!duplicated(text),]
    temp_txt$count = str_count(temp_txt$text,good_string) - str_count(temp_txt$text,bad_string)
    
    if(any(temp_txt$count[!temp_txt$looks_like_frontmatter>0])){
      temp_txt = temp_txt[!temp_txt$looks_like_frontmatter,]
    }
    if(all(temp_txt$count<=0)&any(temp_txt$keep)){
      temp_txt = temp_txt[keep==1,]  
    }
    temp_txt[,.(Page,count,keep,keep2)]
    temp_txt = temp_txt[Page>=min(temp_txt$Page[which(temp_txt$count==max(temp_txt$count))]),]
    
    if(any(temp_txt$keep!=0)){
      add = sort(unique(c(sapply(which(temp_txt$keep==1), function(x) x + 0:2))))
      add = add[add<=nrow(temp_txt)]
      temp_txt$keep[add]<-1
    }
    if(any(temp_txt$keep2!=0)){
      add = sort(unique(c(sapply(which(temp_txt$keep2==1), function(x) x + 0:2))))
      add = add[add<=nrow(temp_txt)]
      temp_txt$keep2[add]<-1
    }
    if(any(temp_txt$keep>0|temp_txt$keep2>0)){
      temp_txt = temp_txt[keep==1|keep2==1,]}
    if(all(temp_txt$keep==0&temp_txt$keep2==0)){
      temp_txt = temp_txt[count>0,]
    }
    
    temp_txt = temp_txt[keep!=0|keep2!=0|temp_txt$count>0,]
    #  temp_txt = temp_txt[!grepl('\\.{8,}',text),]
    temp_txt = temp_txt[!{grepl('References Cited|Works Cited|REFERENCES|Index|INDEX',temp_txt$text) & !grepl('Preparers|Contributors|PREPARERS|CONTRIBUTORS',temp_txt$text)},]
  }
  temp_txt
},cl = 6)


pages_dt = rbindlist(pages_list,use.names = T,fill = T)
pages_dt = pages_dt[str_extract(pages_dt$FILE,'^[0-9]{8}') %in% epa$EIS.Number,]
pages_dt$USE_KEEP2 = pages_dt$FILE %in% pages_dt[,list(sum(keep),sum(keep2)),by=.(FILE)][V1>10&V2>0]$FILE
pages_dt = pages_dt[{!USE_KEEP2}|keep2==1,]

by_file = split(pages_dt,pages_dt$FILE)

raw_pages = pblapply(seq_along(by_file),function(i){
  pdf_file = str_replace(by_file[[i]]$FILE[1],'txt$','pdf')
  floc = pdf_file_list[match(pdf_file,base_name)]
  text_pages = tryCatch({pdftools::pdf_text(floc)[by_file[[i]]$Page]},
                        error = function(e) NULL)
  text_pages},cl = 6)

raw_pages[sapply(raw_pages,is.null)] <- NA

if(length(by_file)==length(raw_pages)){
  for(i in 1:length(raw_pages)){
    by_file[[i]]$text <- raw_pages[[i]]
  }
}
raw_pages_dt = rbindlist(by_file,use.names = T,fill = T)

saveRDS(raw_pages_dt,'../tuolumne/boilerplate_project/input/detected_preparer_pages_uncleaned.RDS')

