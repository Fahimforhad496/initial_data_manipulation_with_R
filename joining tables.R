install.packages("tidyverse")
library(sparklyr)
library(dplyr)
library(ggplot2)
spark_install()
sc <- spark_connect(master = "local")

claimTable <- spark_read_csv(sc,"claimTable", "Merged_basic_info_dataset/part-00000-14b4f071-81e5-4f57-9697-f42d66bbaa81-c000.csv")
transectionTable <- spark_read_csv(sc, "transectionTable","total_transaction.csv")

fulljoinDf <- full_join(claimTable,transectionTable, by = "Claim_No_")
sdf_dim(fulljoinDf); sdf_nrow(fulljoinDf); sdf_ncol(fulljoinDf)
View(fulljoinDf)
head(fulljoinDf)

