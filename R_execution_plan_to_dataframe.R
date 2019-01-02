# Reading Execution plan
setwd("C:\\Users\\Tomaz\\Desktop")

library(XML)
#library(readr)
#library(methods)

# Give the input file name to the function.
# and get the query statement out
result <- xmlParse(file = "plan1.sqlplan")
sql <- xpathSApply(result, "//ns:ShowPlanXML/ns:BatchSequence/ns:Batch/ns:Statements/ns:StmtSimple/@StatementText", namespaces = c(ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"))
cat(sql)


# Exract the root node form the xml file.
# get clean XML
rootnode <- xmlRoot(result)
# clean xml
rootnode


#xml to list
xml_data <- xmlToList(rootnode)


#Attributes of each node
RelOp <- xml_data$BatchSequence$Batch$Statements$StmtSimple$QueryPlan$RelOp$.attrs

RelOps <- xml_data$BatchSequence$Batch$Statements$StmtSimple$QueryPlan$RelOp$NestedLoops$RelOp


df <- data.frame(matrix(unlist(RelOps$NestedLoops$RelOp$.attrs), nrow = 22, byrow = TRUE))



library(plyr)
df2 <- ldply(RelOps, data.frame)



xmlfile <- xmlTreeParse(result)
xmltop <- xmlRoot(xmlfile)
xmlSApply(xmltop, function(x) xmlSApply(x,xmlValue))

dataFrame <- xmlSApply(rootnode, function(x) xmlSApply(rootnode, xmlValue))
data.frame(t(dataFrame),row.names=NULL)

xmlS
