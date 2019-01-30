library(DatabaseConnector)
devtools::install_github("OHDSI/CohortMethod")
library(CohortMethod)
devtools::install_github("OHDSI/SqlRender")
library(SqlRender)

##Configuring the connection to the server
dbms <- 'sql server' #sql server, postgres, etc
server <- 'nypcdwdbtst1.sis.nyp.org'
file <- 'C:/Users/tf2428/Documents/Network Studies/Spotnitz/ThreeDrenderingNoEcho.csv'

connectionDetails <- createConnectionDetails(dbms = dbms,
                                             server = server)

cdmDatabaseSchema <- "ohdsi_cumc_pending.dbo"
vocabularyDatabaseSchema <- 'ohdsi_cumc_pending.dbo'
resultsDatabaseSchema <- "ohdsi_cumc_pending.results"
targetCohortTable <- "ThreeDrenderingNoEcho"
targetCohortId <- "1"



sql <- readSql("C:/Users/tf2428/Documents/Network Studies/Spotnitz/ThreeDrenderingNoEcho_parameterized.sql")
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

write.csv(x = output, file = file)

