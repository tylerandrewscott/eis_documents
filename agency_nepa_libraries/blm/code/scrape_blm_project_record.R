

system('docker run -d -p 4445:4444 selenium/standalone-chrome')
library(data.table)
library(Rcrawler)
library(tokenizers)
library(rvest)
library(readxl)
library(XML)
library(data.table)
require(RSelenium)
require(seleniumPipes)
library(data.table)
library(RSelenium)


project_record_file = 'agency_nepa_libraries/blm/metadata/new_project_record.csv'
project_record_file = 'agency_nepa_libraries/blm/metadata/project_record.csv'
t1 <- fread(project_record_file)
t2 <- fread(project_record_file2)

if(file.exists(project_record_file)){precord = fread(project_record_file)}else{precord = data.table()}



remDr <- RSelenium::remoteDriver(remoteServerAddr = "localhost",
                                 port = 4445L,
                                 browserName = "chrome")
remDr$open()

https://eplanning.blm.gov/eplanning-ws/epl/ref/getStateCodes
https://eplanning.blm.gov/eplanning-ws/epl/ref/getFiscalYears

https://eplanning.blm.gov/eplanning-ui/search?filterSearch=%7B%22states%22:null,%22projectTypes%22:%5B8%5D,%22programs%22:null,%22years%22:null,%22open%22:false,%22active%22:false%7D


start = 'https://eplanning.blm.gov/eplanning-ui/home'
sess<-session(start)
fm<-html_form(sess)
read_

remDr$navigate(start)
# 
# state_selector = remDr$findElement('id','filter-states')
# state_arrow = state_selector$findChildElement('css selector','#filter-states > div > span')
# state_arrow$clickElement()
# state_selector2 = remDr$findElement('class','ng-input')
# raw_source = state_selector2$getPageSource()
# filter_active = remDr$findElement('css selector','#filter-active')
# filter_active$clickElement()

require(stringr)

office_options = fromJSON('https://eplanning.blm.gov/eplanning-ws/epl/ref/getStateCodes')


#state_selector = remDr$findElement('id','filter-states')
#state_arrow = state_selector$findChildElement('css selector','#filter-states > div > span')
#state_arrow$clickElement()



prefix ='https://eplanning.blm.gov/eplanning-ui/search?filterSearch=%7B%22states%22:%5B%22'
suffix ='%22%5D,%22offices%22:null,%22projectTypes%22:null,%22programs%22:null,%22years%22:null,%22open%22:false,%22active%22:false%7D'

#all_dt = data.table()
for(state in office_options$dataName){
  print(state)
  url = paste0(prefix,state,suffix)
  remDr$navigate(url)
  Sys.sleep(30)
  remDr$screenshot(display = T)
  source = remDr$getPageSource()
  ht = source[[1]] %>% read_html()
  temp_pids = ht %>% html_nodes('.ag-cell-value:nth-child(1)') %>% html_text(trim = T)
  if(length(temp_pids)==0){next}
  rowCount = remDr$findElement('css selector','.small')
  max_pages = ceiling(as.numeric(str_extract(str_extract(rowCount$getElementText()[[1]],'[0-9]{1,}\\srows$'),'[0-9]{1,}'))/10)
  current_page = 1
  state_dt = data.table()
  for(p in current_page:max_pages){
    print(p)
    next_page = remDr$findElement('link text',as.character(p))
    next_page$clickElement()
    Sys.sleep(2)
    source = remDr$getPageSource()
    ht = source[[1]] %>% read_html()
    pids = ht %>% html_nodes('.ag-cell-value:nth-child(1)') %>% html_text(trim = T)
    plinks = ht %>% html_nodes('.ag-cell-value a')
    link = plinks %>% html_attr('href')
    pnames = plinks %>% html_text(trim = T)
    pnames = gsub('\\"','',pnames)
    tdt = data.table(pids,link,pnames)
    state_dt = rbind(state_dt,tdt)
  }
  all_dt = rbind(all_dt,state_dt)
}

all_dt = all_dt[!duplicated(all_dt),]

fwrite(all_dt,file = project_record_file)


  