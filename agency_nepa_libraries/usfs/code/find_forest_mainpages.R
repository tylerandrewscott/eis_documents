# ###two packages that will do most of what you need
library(tidyverse)
library(rvest)
library(rvest)
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
library(httr)
# #progress bar lapply, useful for gauging how long/bad your code is in an apply loop
library(pbapply)
# #page listing forest urls
nfs_site_list = "https://web.archive.org/web/20190701045610/https://www.fs.fed.us/recreation/map/state_list.shtml"
sites = nfs_site_list %>% read_html() %>% html_nodes('a')
site_matches = grep('National Forest$|National Forests$|in Texas$|Management Area|National Gra[s]{1,}land|National Tallgrass Prairie',sites %>% html_text(trim=T),value=F)
text = sites %>% html_text(trim=T)
hrefs = sites %>% html_attr('href')

site_dfs = data.frame(Forest = text[site_matches],url = hrefs[site_matches],stringsAsFactors = F) %>% filter(!duplicated(.))

site_dfs$url = str_extract(site_dfs$url,'[^^]http.+')
#site_dfs$url[!grepl('^http',site_dfs$url)] <- paste0('http://www.fs.fed.us',site_dfs$url[!grepl('^http',site_dfs$url)])
site_dfs = site_dfs[!duplicated(site_dfs),]
site_dfs$url = gsub('^\\/','',site_dfs$url)
site_dfs$url = gsub('http:','https:',site_dfs$url)


site_dfs$url[site_dfs$url =="https://www.fs.fed.us/r9/white/" ] <- 'https://www.fs.usda.gov/land/whitemountain'
site_dfs$url[site_dfs$url =="https://www.fs.fed.us/r5/sequoia" ] <- 'https://www.fs.usda.gov/sequoia'
site_dfs$url[site_dfs$url =="https://www.fs.fed.us/r3/sfe/"  ] <- 'https://www.fs.usda.gov/santafe'
site_dfs$url[site_dfs$url =="https://www.fs.fed.us/r1/clearwater/"  ] <- 'https://www.fs.usda.gov/clearwater'
site_dfs$url[site_dfs$url =="https://www.fs.fed.us/r10/tongass/"   ] <- 'https://www.fs.usda.gov/tongass'
site_dfs$url[site_dfs$url ==  "https://www.fs.fed.us/r1/helena/"  ] <- 'https://www.fs.usda.gov/helena'


# # #some urls redirect to a different page, so use session to find redirect
fsites = site_dfs$url

forest_names_url = pblapply(fsites, function(x) html_session(x)$url)
# #some redirects result in duplicates
forest_names_url = unique(unlist(forest_names_url))
# # 
# # 
forest_names_url = forest_names_url[!duplicated(gsub('\\/$','',forest_names_url))]
# ##isolate names used on project page urls
forest_name_urls = gsub("\\/","",gsub('main\\/|\\/home|land\\/','',gsub("https://www.fs.usda.gov/",'',forest_names_url)))
forest_name_urls = append(forest_name_urls,'ochoco')

prefix = "https://www.fs.usda.gov/wps/portal/fsinternet/cs/projects/"
suffix = "/landmanagement/projects?archive=1&sortby=1"
proj_archives = paste0(prefix,forest_name_urls,suffix)

# ##isolate names used on project page urls
prefix = "https://www.fs.usda.gov/projects/"
suffix = "/landmanagement/projects"
proj_current = paste0(prefix,forest_name_urls,suffix)

proj_url = data.frame(project_urls = c(proj_archives,proj_current),status = c(rep('Archived',length(proj_archives)),
                                                                              rep('Current',length(proj_current))),stringsAsFactors = F)

proj_url$nf = gsub('https://www.fs.usda.gov/projects/','',proj_url$project_urls,fixed=T)
proj_url$nf = gsub('/landmanagement/projects','',proj_url$nf,fixed=T)
proj_url$nf = gsub('https://www.fs.usda.gov/wps/portal/fsinternet/cs/projects/','',proj_url$nf,fixed=T)
proj_url$nf = gsub('?archive=1&sortby=1','',proj_url$nf,fixed=T)


base = 'https://www.fs.fed.us/nepa/nepa_home.php'
port = 4567
pJS <- wdman::phantomjs()
#pJS <- wdman::phantomjs()
Sys.sleep(5) # give the binary a moment
remDr <- remoteDriver(port=port)
remDr$open()
remDr$navigate(base)
base
nf_options = remDr$findElement(using = 'name','forest')
options = nf_options$selectTag()


currents = paste0('https://www.fs.fed.us/nepa/project_list.php?forest=',options$value)
archives = paste0('https://www.fs.fed.us/nepa/project_list.php?forest=',options$value,'&archive=1')

tdt = data.table(project_urls = c(currents,archives),status = c(rep('Current',length(currents)),rep('Archived',length(currents))),
           nf = options$text)
forest_names_url2 = pbsapply(tdt$project_urls, function(x) html_session(x)$url,cl = 1)
tdt$project_urls <- unlist(forest_names_url2)

proj_url = rbindlist(list(proj_url,tdt),use.names = T)
proj_url = proj_url[!duplicated(proj_url$project_urls),]

write_csv(x = proj_url,file = 'agency_nepa_libraries/usfs/metadata/forest_project_page_urls.csv')


