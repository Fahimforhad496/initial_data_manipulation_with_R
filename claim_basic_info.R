#Include libraries and create a connection

install.packages("tidyverse")
library(sparklyr)
library(dplyr)
library(ggplot2)
spark_install()
sc <- spark_connect(master = "local")

#Load claim dataset

claim_main <-spark_read_csv(sc,"claim_main", path = "claim.csv")

#checking spark "claim_main"

sdf_dim(claim_main); sdf_nrow(claim_main); sdf_ncol(claim_main)
glimpse(claim_main)

print(claim_main, n=2, width = Inf)

head(claim_main)

View(claim_main)

claim_main <- select(claim_main, Client_Name, AYr, Previous_Member, Claim_No_, Claim_Date:SubClass, Claim_Status, Injury_Damage:Body_Location)

head(claim_main)
spark_write_csv(claim_main, "basic_info_dataset/",header = TRUE, delimiter = ",", quote = "\"",escape = "\\", charset="UTF-8",mode=NULL)

new_claim_ds <- spark_read_csv(sc, "new_claim_ds", "basic_info_dataset/*.csv")

sdf_dim(new_claim_ds); sdf_nrow(new_claim_ds); sdf_ncol(new_claim_ds)

glimpse(new_claim_ds)

print(new_claim_ds, n=2, width = Inf)

head(new_claim_ds)

new_claim_ds<- sdf_coalesce(new_claim_ds, partitions = 1)

spark_write_csv(new_claim_ds, "Merged_basic_info_dataset/")
