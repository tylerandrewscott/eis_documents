require(rvest)
require(data.table)
require(stringi)
mta = readRDS('meta_data.RDS')


db = fread('input/integrity_db/integrity15.csv')
db = db[-c(1:3),]
db = db[op_status=='Certified',]

oid = 'https://organic.ams.usda.gov/integrity/'
oid_page = html_session(oid)

certifier_selector = '#ctl00_MainContent_CertifyingAgentCombo_DropDown'
find_certifiers = oid_page %>% read_html() %>% 
  html_nodes(certifier_selector) %>%
  html_text(trim = T)

abb = str_extract_all(find_certifiers ,'((?!\\[)[A-Z|\\-|\\s]+(?=\\]))')[[1]]
name = str_split(find_certifiers ,'(\\[[A-Z|\\-|\\s]+\\])')[[1]][-1]
name = str_remove(name,'^\\s')
certifiers = data.table(abb,name)
#drop the "transitioning" label
certifiers = certifiers[certifiers$abb!='---']       
db$cert_abb = certifiers$abb[match(db$Cert_name,certifiers$name)]
db$cert_abb[is.na(db$cert_abb)&grepl('Nevada',db$Cert_name)] <- 'NDA'
db$cert_abb[is.na(db$cert_abb)&grepl('Utah',db$Cert_name)] <- 'UDAF'
db$cert_abb[is.na(db$cert_abb)&grepl('Primus',db$Cert_name)] <- 'PL'
db$cert_abb[is.na(db$cert_abb)&grepl('MOFGA',db$Cert_name)] <- 'MOFGA'
db$cert_abb[is.na(db$cert_abb)&grepl('Global Organic Alliance',db$Cert_name)] <- 'GOA'
db$cert_abb[is.na(db$cert_abb)&grepl('Kiwa',db$Cert_name)] <- 'BCS'
db$cert_abb[is.na(db$cert_abb)&grepl('ECOCERT ICO',db$Cert_name)] <- 'ECO ICO'
db$cert_abb[is.na(db$cert_abb)&grepl('^International Certification Services',db$Cert_name)] <- 'ICS'
db$cert_abb[is.na(db$cert_abb)&grepl('Certification of Environmental Standards',db$Cert_name)] <- 'CERES'
db$cert_abb[is.na(db$cert_abb)&grepl('Northeast Organic Farming Association',db$Cert_name)] <- 'NOFA-NY'
db$cert_abb[is.na(db$cert_abb)&grepl('Idaho State',db$Cert_name)] <- 'ISDA'
db$cert_abb[is.na(db$cert_abb)&grepl('New Mexico',db$Cert_name)] <- 'NMDA'
db$cert_abb[is.na(db$cert_abb)&grepl('Georgia Crop Improvement',db$Cert_name)] <- 'GCIA'
db$cert_abb[is.na(db$cert_abb)&grepl('Pro-Cert',db$Cert_name)] <- 'PRO'
db$cert_abb[is.na(db$cert_abb)&grepl('Eco-Logica',db$Cert_name)] <- 'LOGI'
db$cert_abb[is.na(db$cert_abb)&grepl('IBD',db$Cert_name)] <- 'IBD'
db$cert_abb[is.na(db$cert_abb)&grepl('SCS Global',db$Cert_name)] <- 'SCS'
db$cert_abb[is.na(db$cert_abb)&grepl('Stellar',db$Cert_name)] <- 'STEL'
db$cert_abb[is.na(db$cert_abb)&grepl('Global Culture',db$Cert_name)] <- 'GLO'
db$cert_abb[is.na(db$cert_abb)&grepl('Natures International Certification Services',db$Cert_name)] <- 'NICS'
db$cert_abb[is.na(db$cert_abb)&grepl('Organizacion Internacional Agropecuaria',db$Cert_name)] <- 'OIA'
db$cert_abb[is.na(db$cert_abb)&grepl('Bio Latina',db$Cert_name)] <- 'BIOL'
db$cert_abb[is.na(db$cert_abb)&grepl('Australian Certified Organic',db$Cert_name)] <- 'ACO'
db$cert_abb[is.na(db$cert_abb)&grepl('Certificadora Mexicana',db$Cert_name)] <- 'CMEX'
db$cert_abb[is.na(db$cert_abb)&grepl('Tse-Xin',db$Cert_name)] <- 'TOC'

ops = db[,.N,by=.(cert_abb)]
setnames(ops,'N','Operations')
notes = meta_dt[,.N,by = .(Certifier)]
setnames(notes,'N','NONCs')

db[grepl('Natures International',Cert_name),]
notes$Total_Ops = ops$Operations[match(notes$Certifier,ops$cert_abb)]

require(htmlTable)


tableout <-htmlTable(notes[order(-NONCs)])
outdir.tables = "output/" 
sink(paste0(outdir.tables,"noncs_totalops_table.html"))
print(tableout,type="html",useViewer=F)
sink()

require(tidyverse)

ggplot(notes,aes(x = log(Total_Ops),y= NONCs)) + geom_point()



tableOut


notes[order(-NONCs)][is.na(Total_Ops)]


notes[Certifier=='']

certifiers[abb=='OIA']
db[is.na(cert_abb),.N,by=.(Cert_name)][order(-N)]

test = toupper(unique(stri_enc_toascii(db$Cert_name)))

certifiers
certifiers[abb=='SCS']
meta_dt[Certifier=='MOFGA']
match('Midwest Organic Services Association, Inc.','Midwest Organic Services Association, Inc.')


certifiers[abb=='MOSA']
test[!test %in% toupper(certifiers$name)]
certifiers
db$Cert_name[27674]
db$Cert_name

stri_enc_to
fread('input/')
db[,.N,by=.(Cert_name)]

db[,.N,by=.(op_status)]
