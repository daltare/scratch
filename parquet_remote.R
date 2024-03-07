# test different ways to connect to remote parquet files


# version 1 ---------------------------------------------------------------
# https://rfsaldanha.github.io/posts/query_remot_parquet_file.html
parquet_url <- "https://github.com/rfsaldanha/releases/releases/download/v1/flights.parquet"

conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = ":memory:"
)

DBI::dbExecute(conn, "INSTALL httpfs;")
DBI::dbExecute(conn, "LOAD httpfs;")

res <- DBI::dbGetQuery(
    conn, 
    glue::glue("SELECT carrier, flight, tailnum, year FROM '{parquet_url}' WHERE year = 2013 LIMIT 100")
)

dplyr::glimpse(res)



# version 2 ---------------------------------------------------------------
# https://r.iresmi.net/posts/2023/fast_remote_parquet/
dataset <- "https://static.data.gouv.fr/resources/bureaux-de-vote-et-adresses-de-leurs-electeurs/20230626-135723/table-adresses-reu.parquet"


library(duckdb)
library(tidyverse)
library(glue)

cnx <- dbConnect(duckdb::duckdb())
# To do once:
dbExecute(cnx, "INSTALL 'httpfs';")
dbExecute(cnx, "LOAD 'httpfs';")

dbSendQuery(cnx, glue("
  CREATE VIEW bureaux AS
    SELECT * 
    FROM '{dataset}'"))

# dbExecute(cnx, "INSTALL 'json';")
# dbExecute(cnx, "LOAD 'json';")



# version 3 ---------------------------------------------------------------

# https://francoismichonneau.net/2023/06/duckdb-r-remote-data/
library(DBI)
library(duckdb)
library(dplyr)

# con <- duckdb::duckdb()
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "INSTALL httpfs;")
dbExecute(con, "LOAD httpfs;")


dbGetQuery(con,
           "SELECT species,
          AVG(bill_length_mm) AS avg_bill_length,
          AVG(bill_depth_mm) AS avg_bill_depth
   FROM PARQUET_SCAN('https://francoismichonneau.net/assets/data/penguins.parquet')
   GROUP BY species;")

dbExecute(con,
          "CREATE VIEW penguins AS
   SELECT * FROM PARQUET_SCAN('https://francoismichonneau.net/assets/data/penguins.parquet');
")
dbListTables(con)

tbl(con, "penguins") |>
    group_by(species) |>
    summarize(
        avg_bill_length = mean(bill_length_mm),
        avg_bill_depth = mean(bill_depth_mm)
    )





# testing -----------------------------------------------------------------

## Version 1 - WORKS ----
parquet_url <- "https://github.com/daltare/scratch/raw/main/penguins.parquet"

conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = ":memory:"
)

DBI::dbExecute(conn, "INSTALL httpfs;")
DBI::dbExecute(conn, "LOAD httpfs;")

res <- DBI::dbGetQuery(
    conn, 
    glue::glue("SELECT bill_length_mm, bill_depth_mm, species FROM '{parquet_url}' WHERE species = 'Chinstrap'")
)

dplyr::glimpse(res)



## Version 3 - WORKS ----
library(DBI)
library(duckdb)
library(dplyr)
library(glue)

# con <- duckdb::duckdb()
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "INSTALL httpfs;")
dbExecute(con, "LOAD httpfs;")
url_test <- 'https://github.com/daltare/scratch/raw/main/penguins.parquet'
# 'https://github.com/daltare/scratch/raw/main/penguins.parquet' # works
# 'https://raw.githubusercontent.com/daltare/scratch/main/penguins.parquet' # works


dbGetQuery(con,
           glue("SELECT species,
          AVG(bill_length_mm) AS avg_bill_length,
          AVG(bill_depth_mm) AS avg_bill_depth
   FROM PARQUET_SCAN('{url_test}')
   GROUP BY species;"))

dbExecute(con,
          glue("CREATE VIEW penguins AS
   SELECT * FROM PARQUET_SCAN('{url_test}');
"))
dbListTables(con)

tbl(con, "penguins") |>
    group_by(species) |>
    summarize(
        avg_bill_length = mean(bill_length_mm),
        avg_bill_depth = mean(bill_depth_mm)
    )
