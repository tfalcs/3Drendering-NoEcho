##Installing and loading required R packages
install.packages("devtools")
library(devtools)
devtools::install_github("OHDSI/DatabaseConnector")
library(DatabaseConnector)
devtools::install_github("OHDSI/CohortMethod")
library(CohortMethod)
devtools::install_github("OHDSI/SqlRender")
library(SqlRender)


##Configuring the connection to the server
dbms <- 'sql dialect' #sql server, postgres, etc
server <- 'server name' 
user <- 'user'
password <- 'password'


##Setting up the directory
folder <- 'C:/user/studyfolder' #specify a folder to store files and the output
file <- c('ThreeDrenderingNoEcho_parameterized.sql') #the sql script that was downloaded from OHDSI github


##Connecting to your database
connectionDetails <- createConnectionDetails(dbms = dbms,
                                             user = user,
                                             password = password,
                                             server = server)


##Fill in the names of your cdm and results schemas
cdmDatabaseSchema <- "cdm_schema"
vocabularyDatabaseSchema <- 'cdm_schema'
resultsDatabaseSchema <- "results_schema"
targetCohortTable <- "ThreeDrenderingNoEcho"
targetCohortId <- "1"


##Using sqlRender for the sql script
sql <- readSql(file)
sql <- renderSql(sql, cdm_database_schema = cdmDatabaseSchema, vocabulary_database_schema = vocabularyDatabaseSchema,
                 target_database_schema = resultsDatabaseSchema, target_cohort_table = targetCohortTable, target_cohort_id = targetCohortId)$sql
sql <- translateSql(sql, targetDialect = connectionDetails$dbms)$sql

connection <- connect(connectionDetails)
executeSql(connection, sql)


sql <- paste("SELECT year(cohort_start_date) as year, count(*) as freq",
             "FROM @target_database_schema.@target_cohort_table",
             "GROUP BY year(cohort_start_date)",
             "ORDER BY year(cohort_start_date)")
sql <- renderSql(sql, target_database_schema = resultsDatabaseSchema, target_cohort_table = targetCohortTable)$sql
sql <- translateSql(sql, targetDialect = connectionDetails$dbms)$sql
output <- querySql(connection, sql)
output <- data.frame(output)

##Placing the year counts in the folder specified above
write.csv(x = output, file = file)

