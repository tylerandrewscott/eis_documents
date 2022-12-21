require(data.table)
require(pbapply)
require(RCurl)
#require(boxr)
library(tidyverse)
library(stringr)
library(rvest)

doe_ea_base = 'https://www.energy.gov/nepa/doe-environmental-assessments'
ea_session = session(doe_ea_base)
ea_lines = readLines(doe_ea_base)

internal_table <- str_extract_all(ea_lines,"http.*.csv")
#ea_csv = gsub('\"','',str_extract(grep('Sheet1.csv',ea_lines,value = T),'\".*\"'))
doe_ea =  fread(unlist(internal_table))

doe_eis_base = 'https://www.energy.gov/nepa/doe-environmental-impact-statements'
eis_session = html_session(doe_eis_base)
eis_lines = readLines(doe_eis_base)
internal_table <- str_extract_all(eis_lines,"http.*.csv")
#eis_csv = gsub('\"','',str_extract(grep('Sheet1.csv',eis_lines,value = T),'\".*\"'))
doe_eis =  fread(unlist(internal_table))
doe_ea$Project_Type = 'EA'
doe_eis$Project_Type = 'EIS'

setnames(doe_eis,"EIS-#",'NEPA_ID')
setnames(doe_ea,"EA#",'NEPA_ID')
doe = rbindlist(list(doe_eis,doe_ea),use.names = T)
doe$full_url = NA
doe$project_description = NA
project_meta = 'agency_nepa_libraries/doe/metadata/doe_nepa_record.RDS'

pref = 'https://www.energy.gov/'

current = readRDS(project_meta)
doe_need <- doe[!basename(doe$NEPA_ID) %in% current$NEPA_ID,]

doe_need$URL <- gsub('enery\\.gov','energy.gov',doe_need$URL)
library(pbapply)
for(i in 1:nrow(doe_need)) {
  if(!httr::http_error(doe_need$URL[i]))
  { print(i)
    temp_session = session(doe_need$URL[i])
    doe_need$full_url[i] <- temp_session$url
    temp_html = temp_session %>% read_html()
    pjd = temp_html %>% html_nodes('p') %>% html_text(trim=T) %>% .[.!=''] %>% paste(.,collapse= ' ')
    doe_need$project_description[i] <- pjd
  }
}
doe_need$YEAR = doe_need$Date

doe_new <- rbind(current,doe_need,use.names = T,fill = T)
saveRDS(doe_new,project_meta)


