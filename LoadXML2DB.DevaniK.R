##---------------------------------------------------------
##
##course : "CS5200 Database Management System"
##title : "Practicum II: Part 1 - Load XML Data into Database"
##author : "Krina Devani (devani.k@northeastern.edu), Sarthak Kagliwal (kagliwal.s@northeastern.edu)"
##term: "Spring 2023"
##date : "20th April, 2023"
##
##Note : 
## 1. we have taken 03 June, 2000 as a default date.
## 2. we have taken first year, first month or first day if medlinedate is provided.
##--------------------------------------------------------

#Library
library(XML)
library(RSQLite)
library(xml2)

# Create a new database 
mydb <- dbConnect(RSQLite::SQLite(), "articles.db")

#Read XML

tf.dir <- "download.zip.xml"
if (!dir.exists(tf.dir)) {
  dir.create(tf.dir)
}

## Download the zip file
zip.file.name <- "data.zip"
url.zip <- paste0("http://raw.githubusercontent.com/sarthak55k/PracticumDatabase/f906de923839cf0f3e01033b8296862a002ec46f/", zip.file.name)
tf.zip.file <- paste0(tf.dir,"/",zip.file.name)

download.file(url = url.zip, 
              destfile = tf.zip.file, 
              quiet = T)

## Unzip the temp folder
xml_files <- unzip(tf.zip.file, exdir = tf.dir)

## wait until the file exists and then parse
if (file.exists(xml_files[1])) {
  ## Parse the first file
  xmlDOM <- xmlParse(xml_files[1], validate= T)
}

## Delete temporary files and folder
unlink(tf.dir, T, T)

## check that the XML parsed properly
root<- xmlRoot(xmlDOM)

#doc <- xmlParse("pubmed-tfm-xml.xml",validate = T)
#root <- xmlRoot(doc)

# Query to create tables
queryArticle <- "CREATE TABLE Articles(
  PMID INTEGER PRIMARY KEY,
  ArticleTitle VARCHAR(255),
  JournalIssueID INT,
  CompleteYN CHAR(1),
  FOREIGN KEY (JournalIssueID) REFERENCES Journal(JournalIssueID)
)"

queryJournal <- "CREATE TABLE Journals(
  JournalID INTEGER PRIMARY KEY AUTOINCREMENT,
  ISSN VARCHAR(255),
  ISSNType VARCHAR(255),
  Title VARCHAR(255),
  ISOAbbreviation VARCHAR(255),
  CitedMedium VARCHAR(255)
)"

queryJournalIssue <- "CREATE TABLE JournalIssue(
  JournalIssueID INTEGER PRIMARY KEY AUTOINCREMENT,
  JournalID INTEGER,
  Volume VARCHAR(255),
  Issue VARCHAR(255),
  PubYear VARCHAR(255),
  PubMonth VARCHAR(255),
  PubDay VARCHAR(255)
)"

queryAuthorArticle <- "CREATE TABLE AuthorArticle (
  PMID INT,
  AuthorID INT,
  FOREIGN KEY (PMID) REFERENCES Article(PMID)
  FOREIGN KEY (AuthorID) REFERENCES Authors(AuthorID)
)"

queryAuthor <- "CREATE TABLE Authors(
  AuthorID INTEGER PRIMARY KEY AUTOINCREMENT,
  LastName VARCHAR(255),
  ForeName VARCHAR(255),
  Initials VARCHAR(255),
  Suffix VARCHAR(255),
  CollectiveName VARCHAR(255),
  AffiliationInfo VARCHAR(255),
  ValidYN CHAR(1)
)"

# Drop queries if table exists
dbExecute(mydb, "DROP TABLE IF EXISTS Articles")
dbExecute(mydb, "DROP TABLE IF EXISTS Journals")
dbExecute(mydb, "DROP TABLE IF EXISTS JournalIssue")
dbExecute(mydb, "DROP TABLE IF EXISTS AuthorArticle")
dbExecute(mydb, "DROP TABLE IF EXISTS Authors")

# Create tables
dbExecute(mydb, queryJournal)
dbExecute(mydb, queryJournalIssue)
dbExecute(mydb, queryAuthor)
dbExecute(mydb, queryArticle)
dbExecute(mydb, queryAuthorArticle)


#Helper function
#Parse medlinedate
get_medlinedate <- function(medlineDate){
  date <- list()
  vDate <- strsplit(as.character(medlineDate), '\\s+')
  n <- lengths(vDate)
  vDate <- vDate[[1]]
  year <- "2000"
  month <- "Jun"
  day <- "3"
  if(grepl("^[0-9]{4}", medlineDate)){
    year <- substr(medlineDate, 1 ,4)
    
    if(n > 1 && grepl("^[a-zA-Z]{3}", vDate[2])){
      month <- substr(vDate[2], 1 ,3)
    }
    if(n > 2 && grepl("^[0-9]{2}", vDate[3])){
      day <- substr(vDate[3], 1 ,2)
    }
  } else if (grepl("^[a-zA-Z]{3}", medlineDate)){
    month <- substr(medlineDate, 1 ,3)
    
    if(n > 1 && grepl("^[0-9]{2}", vDate[2])){
      day <- substr(vDate[2], 1 ,2)
    }
  } else if (grepl("^[0-9]{2}", medlineDate)){
    day <- substr(medlineDate, 1 ,2)
  }
  
  date$year <- year
  date$month <- month
  date$day <- day
  
  return (date)
}

#Convert month abbreviation to month number
convert_date <- function(month_name){
  month_integer <- match(tolower(month_name), tolower(month.abb))
  return (month_integer)
}


#Data frame

Journal.df <- data.frame(JournalID = integer(),
                         ISSN = character(),
                         ISSNType = character(),
                         Title = character(),
                         ISOAbbreviation = character(),
                         CitedMedium = character(),
                         stringsAsFactors = F)

Author.df <- data.frame (AuthorID = integer(),
                         LastName = character(),
                         ForeName = character(),
                         Initials = character(),
                         Suffix = character(),
                         CollectiveName = character(),
                         AffiliationInfo = character(),
                         ValidYN = character(),
                         PMID = integer(),
                         stringsAsFactors = F)

main.df <- data.frame(PMID = integer(),
                      ISSN = character(),
                      IssnType = character(),
                      CitedMedium = character(),
                      Volume = character(),
                      Issue = character(),
                      PubYear = character(),
                      PubMonth = character(),
                      PubDay = character(),
                      Title = character(),
                      ISOAbbreviation = character(),
                      ArticleTitle = character(),
                      CompleteYN = character(),
                      stringsAsFactors = F)

#Fetch data from XML to data frame
articles <- xpathApply(root, "./Article")
n <- length(articles)

for(i in 1:n){
  article <- articles[[i]]
  
  pubDetails <- xpathApply(article, "./PubDetails/Journal")[[1]]
  journalIssue <- xpathApply(pubDetails, "./JournalIssue")[[1]]
  pubDate <- xpathApply(journalIssue, "./PubDate")[[1]]
  
  pmid <- xpathSApply(article, "./@PMID")
  issn <- xpathSApply(pubDetails,"./ISSN", xmlValue)
  issnType <- xpathSApply(pubDetails,"./ISSN/@IssnType")
  citedMedium <- xpathSApply(journalIssue,"./@CitedMedium")
  volume <- xpathSApply(journalIssue,"./Volume", xmlValue)
  issue <- xpathSApply(journalIssue,"./Issue", xmlValue)
  
  medlineDate <- xpathSApply(pubDate,"./MedlineDate", xmlValue)
  
  if(length(medlineDate)>0){
    date <- get_medlinedate(medlineDate)
    pubYear <- date$year
    pubMonth <- convert_date(date$month)
    pubDay <- date$day
  }else{
    pubYear <- xpathSApply(pubDate,"./Year", xmlValue)
    
    pubMonth <- xpathSApply(pubDate,"./Month", xmlValue)
    
    if(is.numeric(pubMonth)){
      pubMonth <- convert_date(pubMonth)
    } else {
      pubMonth <- gsub("^0+", "", pubMonth)
    }
    
    pubDay <- xpathSApply(pubDate,"./Day", xmlValue) 
  }
  
  
  title <- xpathSApply(pubDetails,"./Title", xmlValue)
  iSOAbbreviation <- xpathSApply(pubDetails,"./ISOAbbreviation", xmlValue)
  articleTitle <- xpathSApply(article,"./PubDetails/ArticleTitle", xmlValue)
  completeYN <- xpathSApply(article,"./PubDetails/AuthorList/@CompleteYN")
  
  main.df[i,1] <- pmid
  
  if(xmlSize(issn) > 0){
    main.df[i,2] <- issn
  }
  if(xmlSize(issnType) > 0){
    main.df[i,3] <- issnType
  }
  if(xmlSize(citedMedium) > 0){
    main.df[i,4] <- citedMedium
  }
  if(xmlSize(volume) > 0){
    main.df[i,5] <- volume
  }
  if(xmlSize(issue) > 0){
    main.df[i,6] <- issue
  }
  if(xmlSize(pubYear) > 0){
    main.df[i,7] <- pubYear
  }
  if(xmlSize(pubMonth) > 0){
    main.df[i,8] <- pubMonth
  }
  if(xmlSize(pubDay) > 0){
    main.df[i,9] <- pubDay
  }
  if(xmlSize(title) > 0){
    main.df[i,10] <- title
  }
  if(xmlSize(iSOAbbreviation) > 0){
    main.df[i,11] <- iSOAbbreviation
  }
  if(xmlSize(articleTitle) > 0){
    main.df[i,12] <- articleTitle
  }
  if(xmlSize(completeYN) > 0){
    main.df[i,13] <- completeYN
  }
  
  
  #Get author details
  authors <- xpathApply(article, "./PubDetails/AuthorList/Author")
  an <- length(authors)
  if(an > 0){
    for(m in 1:an){
      
      author <- authors[[m]]
      
      Suffix <- xpathSApply(author,"./Suffix", xmlValue)
      LastName <- xpathSApply(author,"./LastName", xmlValue)
      ForeName <- xpathSApply(author,"./ForeName", xmlValue)
      Initials <- xpathSApply(author,"./Initials", xmlValue)
      CollectiveName <- xpathSApply(author,"./CollectiveName", xmlValue)
      AffiliationInfo <- xpathSApply(author,"./AffiliationInfo", xmlValue)
      
      
      if(xmlSize(LastName) > 0){
        Author.df[i,2] <- LastName
      }
      if(xmlSize(ForeName) > 0){
        Author.df[i,3] <- ForeName
      }
      if(xmlSize(Initials) > 0){
        Author.df[i,4] <- Initials
      }
      if(xmlSize(Suffix) > 0){
        Author.df[i,5] <- Suffix
      }
      if(xmlSize(CollectiveName) > 0){
        Author.df[i,6] <- CollectiveName
      }
      if(xmlSize(AffiliationInfo) > 0){
        Author.df[i,7] <- AffiliationInfo
      }
      
      Author.df[i,8] <- xpathApply(author, "./@ValidYN")
      Author.df[i,9] <- xpathApply(article, "./@PMID")
    }
  }
}

#Set default date
main.df$PubYear[is.na(main.df$PubYear) | main.df$PubYear == "" | main.df$PubYear == "N/A"] <- "2000" 
main.df$PubMonth[is.na(main.df$PubMonth) | main.df$PubMonth == "" | main.df$PubMonth == "N/A"] <- "6" 
main.df$PubDay[is.na(main.df$PubDay) | main.df$PubDay == "" | main.df$PubDay == "N/A"] <- "3" 

#Journal data frame
Journal.df <- main.df[, c("ISSN","IssnType","Title","ISOAbbreviation","CitedMedium")]
Journal.df <- unique(Journal.df)
Journal.df$JournalID <- seq.int(nrow(Journal.df))

main.df <- merge(main.df, Journal.df, by = c("ISSN","ISOAbbreviation"))

#Journal issue data frame
JournalIssue.df <- main.df[,c("JournalID", "Volume", "Issue", "PubYear", "PubMonth", "PubDay")]
JournalIssue.df <- unique(JournalIssue.df)
JournalIssue.df$JournalIssueID <- seq.int(nrow(JournalIssue.df))

main.df <- merge(main.df, JournalIssue.df, by = c("JournalID", "Volume", "Issue", "PubYear", "PubMonth", "PubDay"))

#Article data frame
Article.df <- main.df[,c("PMID", "ArticleTitle", "JournalIssueID", "CompleteYN")]

#Author data frame
Author.df <- unique(Author.df)
Author.df$AuthorID <- seq.int(nrow(Author.df))

#AuthorArticle data frame
AuthorArticle.df <- Author.df[, c("AuthorID", "PMID")]

Author.df <- Author.df[, !(names(Author.df) %in% c("PMID"))]

#Add data in to the tables from data frame
dbWriteTable(mydb, "Authors", Author.df, overwrite = T)
dbWriteTable(mydb, "AuthorArticle", AuthorArticle.df, overwrite = T)
dbWriteTable(mydb, "Journals", Journal.df, overwrite = T)
dbWriteTable(mydb, "JournalIssue", JournalIssue.df, overwrite = T)
dbWriteTable(mydb, "Articles", Article.df, overwrite = T)

#Print few rows of the tables
dbGetQuery(mydb, "SELECT * FROM Articles LIMIT 10")
dbGetQuery(mydb, "SELECT * FROM Journals LIMIT 10")
dbGetQuery(mydb, "SELECT * FROM  JournalIssue LIMIT 10")
dbGetQuery(mydb, "SELECT * FROM  AuthorArticle LIMIT 10")
dbGetQuery(mydb, "SELECT * FROM  Authors LIMIT 10")

#Disconnect database
dbDisconnect(mydb)







