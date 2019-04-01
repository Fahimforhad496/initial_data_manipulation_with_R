install.packages("tidyverse")
library(lubridate)
library(sparklyr)
library(dplyr)
library(ggplot2)
spark_install()
sc <- spark_connect(master = "local")

claimTable <- spark_read_csv(sc,"claimTable", "Merged_basic_info_dataset/basic_info.csv")
transectionTable <- spark_read_csv(sc, "transectionTable","total_transaction.csv")

fulljoinDf <- full_join(claimTable,transectionTable, by = "Claim_No_")
sdf_dim(fulljoinDf); sdf_nrow(fulljoinDf); sdf_ncol(fulljoinDf)
View(fulljoinDf)
head(fulljoinDf)
spark_write_csv(fulljoinDf, "joined tables",header = TRUE, delimiter = ",", quote = "\"",escape = "\\", charset="UTF-8",mode=NULL)

merged_joiningDf <- spark_read_csv(sc, "merged_joiningDf", "joined tables/*.csv")

sdf_dim(merged_joiningDf); sdf_nrow(merged_joiningDf); sdf_ncol(merged_joiningDf)

glimpse(merged_joiningDf)

print(merged_joiningDf, n=2, width = Inf)

head(merged_joiningDf)

merged_joiningDf<- sdf_coalesce(merged_joiningDf, partitions = 1)

spark_write_csv(merged_joiningDf, "Merged_joinedtables/")

df1<-spark_read_csv(sc, "df1", "Merged_joinedtables/part-00000-485bd11d-d268-4b74-aeb7-42efde8e1e04-c000.csv")

df1 <- select(df1, Client_Name:Claimant_Surname, Claim_Status,Transn_Dt,Tr_Posted,Exp_Code,Descrn,Note_Desc)

head(df1)
spark_write_csv(df1, "Trimmed_jointable/",header = TRUE, delimiter = ",", quote = "\"",escape = "\\", charset="UTF-8",mode=NULL)

joined_trimmedTable <- spark_read_csv(sc, "joined_trimmedTable", "Trimmed_jointable/*.csv")

sdf_dim(joined_trimmedTable); sdf_nrow(joined_trimmedTable); sdf_ncol(joined_trimmedTable)

glimpse(joined_trimmedTable)

print(joined_trimmedTable, n=2, width = Inf)

head(joined_trimmedTable)

joined_trimmedTable<- sdf_coalesce(joined_trimmedTable, partitions = 1)

spark_write_csv(joined_trimmedTable, "merged_trimmed_joinedTable")

df2<-spark_read_csv(sc, "df2", "merged_trimmed_joinedTable/part-00000-91cb6c39-498e-4a53-91d4-5b72a36aabe5-c000.csv")
df2 <-select(df2,everything()) %>%
  filter(Exp_Code %in% c("99C Claim","99F Finalise","99I Incident","99R Reopen"))

sdf_dim(df2)
head(df2)

spark_write_csv(df2,"joined_filteredTable")
df3<-spark_read_csv(sc, "df3","joined_filteredTable/*.csv")
df3<-sdf_coalesce(df3,partitions = 1)
spark_write_csv(df3,"ICFR Extracted and merged")
