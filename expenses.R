#connection setup and library inclusion 
install.packages("tidyverse")
library(lubridate)
library(sparklyr)
library(dplyr)
library(ggplot2)
spark_install()
sc <- spark_connect(master = "local")

#Load claim dataset

claim_main <-spark_read_csv(sc,"claim_main", "claim.csv")

#checking spark "claim_main"

sdf_dim(claim_main); sdf_nrow(claim_main); sdf_ncol(claim_main)
glimpse(claim_main)

print(claim_main, n=2, width = Inf)

head(claim_main)

View(claim_main)

claim_main <- select(claim_main, Client_Name, AYr, Claim_No_, Claimant_Paid:Finalised_Date)

head(claim_main)
  spark_write_csv(claim_main, "Expenses Dataset/",header = TRUE, delimiter = ",", quote = "\"",escape = "\\", charset="UTF-8",mode=NULL)

merged_expenses_ds <- spark_read_csv(sc, "merged_expenses_ds","Expenses Dataset/*.csv")

sdf_dim(merged_expenses_ds); sdf_nrow(merged_expenses_ds); sdf_ncol(merged_expenses_ds)

glimpse(merged_expenses_ds)

print(merged_expenses_ds, n=2, width = Inf)

head(merged_expenses_ds)

merged_expenses_ds  <- sdf_coalesce(merged_expenses_ds, partitions = 1)

spark_write_csv(merged_expenses_ds, "Merged expenses dataset")
