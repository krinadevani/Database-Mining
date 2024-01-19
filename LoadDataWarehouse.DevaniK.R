

#Library 
library(RMySQL)
library(DBI)

#Settings
db_user <- 'root'
db_password <- 'Sarthak@ubs5'
db_name <- 'practicum2'
db_host <- '127.0.0.1' 
db_port <- 3306 

#Connect to mySQL database
mySqlDb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                      dbname = db_name, host = db_host, port = db_port)

#Connect SQLite database
sqLiteDb <- dbConnect(RSQLite::SQLite(), dbname = "articles.db")

#Query to create Journal Dimension
queryJournal <- "CREATE TABLE journaldim(
  JournalID INTEGER PRIMARY KEY,
  Title VARCHAR(255) NULL
)"

#Query to create Date Dimension
queryDate <- "CREATE TABLE datedim(
  DateId INTEGER PRIMARY KEY,
  Year VARCHAR(255) NULL,
  Month INTEGER NULL,
  Quarter INTEGER NULL
)"

#Query to create Fact table
queryFact <- "CREATE TABLE fact_table(
  FactID INTEGER PRIMARY KEY,
  JournalID INTEGER NULL,
  DateId INTEGER NULL,
  NumArticles INTEGER,
  NumAuthors INTEGER,
  FOREIGN KEY (JournalID) REFERENCES journaldim(JournalID),
  FOREIGN KEY (DateId) REFERENCES datedim(DateId)
)"

#Delete tables if exists
dbExecute(mySqlDb, "drop table IF EXISTS fact_table")
dbExecute(mySqlDb, "drop table IF EXISTS journaldim")
dbExecute(mySqlDb, "drop table IF EXISTS datedim")

#Create tables
dbExecute(mySqlDb, queryJournal)
dbExecute(mySqlDb, queryDate)
dbExecute(mySqlDb, queryFact)

#Create Index in the fact table
createFactTable.index <- "CREATE INDEX TitleIndex ON fact_table (DateId)"
dbExecute(mySqlDb, createFactTable.index);

#Create Index in the Date dimension table
createDateDim.index <- "CREATE INDEX YearIndex ON datedim (Year)"
dbExecute(mySqlDb, createDateDim.index);

#Query to fetch data from the SQLite database
query <- dbGetQuery(sqLiteDb, "
  SELECT DISTINCT j.JournalID, j.Title, ji.PubYear AS Year, ji.PubMonth AS Month, ((ji.PubMonth-1)/3)+1 AS Quarter,
  COUNT(DISTINCT a.PMID) AS NumArticles, COUNT(DISTINCT aa.AuthorID) AS NumAuthors
   FROM Journals as j
   LEFT JOIN JournalIssue AS ji on j.JournalID = ji.JournalID
   LEFT JOIN Articles AS a on a.JournalIssueID = ji.JournalIssueID
   LEFT JOIN AuthorArticle AS aa on aa.PMID = a.PMID
   GROUP BY j.JournalID, j.Title, ji.PubYear, ji.PubMonth, Quarter
")

#Journal dimension data frame
journal_dim.df <- data.frame(
  JournalID = query$JournalID,
  Title = query$Title
)
journal_dim.df <- unique(journal_dim.df)

#Date dimension data frame
date_dim.df <- data.frame(
  Year = query$Year,
  Month = query$Month,
  Quarter = query$Quarter
)

date_dim.df <- unique(date_dim.df)
date_dim.df$DateId <- seq.int(nrow(date_dim.df))

main.df <- merge(query, journal_dim.df, by = c("JournalID", "Title"))
main.df <- merge(main.df, date_dim.df, by = c("Year", "Month", "Quarter"))

#Fact table data frame
fact.df <- main.df[, c("JournalID", "DateId", "NumArticles", "NumAuthors")]

#Add data in the tables
dbSendQuery(mySqlDb, "SET GLOBAL local_infile = true")
dbWriteTable(mySqlDb, "fact_table", fact.df, overwrite = T)
dbWriteTable(mySqlDb, "journaldim", journal_dim.df, overwrite = T)
dbWriteTable(mySqlDb, "datedim", date_dim.df, overwrite = T)


#Few analytical queries :

#1. What the are number of articles published in every journal in 1975 and 1976?
query1 <- "
  SELECT JournalID, SUM(NumArticles) AS NumArticles FROM fact_table AS f
  LEFT JOIN datedim AS d ON d.DateId = f.DateId
  WHERE d.YEAR ='1975' OR d.YEAR = '1976'
  GROUP BY f.JournalID
  ORDER BY f.JournalID
  LIMIT 100
"
print(dbGetQuery(mySqlDb, query1))

#2. What is the number of articles published in every journal in each quarter of 2012 through 2015?
query2 <- "
  SELECT JournalID, d.YEAR, d.Quarter, SUM(NumArticles) AS NumArticles FROM fact_table AS f
  LEFT JOIN datedim AS d ON d.DateId = f.DateId
  WHERE d.YEAR IN ('1975', '1976', '1977', '1978')
  GROUP BY f.JournalID, d.YEAR, d.Quarter
  ORDER BY f.JournalID, d.YEAR, d.Quarter
"
print(dbGetQuery(mySqlDb, query2))

#3. How many articles were published each quarter (across all years)?
query3 <- "
  SELECT d.YEAR, d.Quarter, SUM(NumArticles) FROM fact_table AS f
  LEFT JOIN datedim AS d ON d.DateId = f.DateId
  GROUP BY d.YEAR, d.Quarter
  ORDER BY d.YEAR, d.Quarter
"
print(dbGetQuery(mySqlDb, query3))

#4. How many unique authors published articles in each year for which there is data?
query4 <- "
  SELECT d.YEAR, SUM(NumAuthors) AS NumAuthors FROM fact_table AS f
  LEFT JOIN datedim AS d ON d.DateId = f.DateId
  GROUP BY d.YEAR
  ORDER BY d.YEAR
"
print(dbGetQuery(mySqlDb, query4))

#Disconnect database
dbDisconnect(mySqlDb)
dbDisconnect(sqLiteDb)

