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

base = 'https://eplanning.blm.gov/epl-front-office/eplanning/lup/lup_register.do'
port = 4567
pJS <- wdman::phantomjs()
Sys.sleep(5) # give the binary a moment
remDr <- remoteDriver(port=port)
remDr$open()
remDr$navigate(base)

#nepaRadio = remDr$findElement(using = 'id',value = 'lupRadioButton')
#nepaRadio$buttondown()

textSearch = remDr$findElement(using = 'partial link text',value = 'Text Search')
textSearch$clickElement()

advSearch = remDr$findElement(using = 'partial link text','Advanced Search')
advSearch$clickElement()

fiscalyears <- remDr$findElement(using = 'xpath',  "//*[(@id = 'fiscalYears')]")
# Convert to html text
fiscal.txt <- fiscalyears$getElementAttribute("outerHTML")[[1]] # gets us the HTML
# Convert to xml
fiscal.xml <- htmlTreeParse(fiscal.txt, useInternalNodes = TRUE) # parse string into HTML tree
# Extract names and values
year.value <- unlist(xpathApply(fiscal.xml, '//*[@value]', xmlGetAttr, 'value'))
year.name <- unlist(xpathApply(fiscal.xml, '//*[@value]', xmlValue, 'value'))
# Put them in a data.table
fiscal <- data.table("fiscalYear" = year.name, "value" = year.value)
fiscal = fiscal[-1,][value!='all',]

### used to select specific years
#option <- remDr$findElement(using = 'xpath', "//*[(@id = 'fiscalYears')]/option[@value = 'all']")
#option$clickElement()
#option <- remDr$findElement(using = 'xpath', "//*[(@id = 'fiscalYears')]/option[@value = '2014']")
#option$clickElement()

sub = remDr$findElement(using = 'class name','epl_register_filter_button')
sub$clickElement()

#textContent > tr:nth-child(3) > td > table:nth-child(4) > tbody > tr:nth-child(2) > td.epl_register_filter_label > input:nth-child(2)
page = remDr$getPageSource() 

proj_table = page[[1]] %>% read_html() %>% 
  html_nodes(css ='#lupResultsTableContainer') %>% html_node('table') %>%
  html_table(trim=T,fill=T) %>% .[[1]]
proj_table$href = page[[1]] %>% read_html() %>% 
  html_nodes(css = '.epl_register_result') %>% html_attr('href')

while(length(remDr$findElements(using = 'partial link text',value = 'next'))!=0){
  new_page = remDr$findElement(using = 'partial link text',value = 'next')  
  new_page$clickElement()  
  temp_page = remDr$getPageSource() 
  temp_table = temp_page[[1]] %>% read_html() %>% 
    html_nodes(css ='#lupResultsTableContainer') %>% html_node('table') %>%
    html_table(trim=T,fill=T) %>% .[[1]]
  temp_table$href = temp_page[[1]] %>% read_html() %>% 
    html_nodes(css = '.epl_register_result') %>% html_attr('href')
  proj_table = rbind(proj_table,temp_table)
}

pjt = data.table(proj_table)
fwrite(x = pjt,file = 'agency_nepa_libraries/blm/metadata/project_record_lup.csv')




