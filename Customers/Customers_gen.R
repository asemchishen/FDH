
##This is a customers data generator
##Purpose: Generated data set may be used in any situation when human personal 
##data is needed.This could be: sales application schemas, demography analitycs etc. 

##Customers data features are:
##last_name - Last name
##first_name - First name
##gender - gender
##dob - date of birth
##phone - phone
##email - email
##ZIP - US zip code
##city - city
##state - US state ID

##IMPORTING EXTERNAL DATA SETS

##Set your working directory where code and data resides
setwd("C:/Users/asemchis/Documents/Datalab/FakeDataHub/Customers")

##Importing Names and Surnames datasets based on US 1990 Census data 
##https://www.census.gov/topics/population/genealogy/data/1990_census/1990_census_namefiles.html
mnames<-read.table("datasets/dist.male.txt", header = FALSE, sep = "", dec = ".")
fnames<-read.table("datasets/dist.female.txt", header = FALSE, sep = "", dec = ".")
snames<-read.table("datasets/dist.all.txt", header = FALSE, sep = "", dec = ".")

##Importing ZIP code locations and dataset from https://simplemaps.com/data/us-zips
zcodes<-read.csv("datasets/uszips.csv", stringsAsFactors = FALSE)
##By default zip codes are read in as integers so "0" in front in are omitted (eg. 00616->616). 
##This code will fix it by converting to char and adding missing zeroes:
zcodes$zip<-as.character(zcodes$zip)
for (i in 1:nrow(zcodes)) {
        if (nchar(zcodes$zip[i])==3) {
                zcodes$zip[i]<-paste0("00",zcodes$zip[i])
        }
        if (nchar(zcodes$zip[i])==4) {
                zcodes$zip[i]<-paste0("0",zcodes$zip[i])
        }
}

##SETTING GENERAL STATISTICAL PARAMETERS

##Gender distribution in customers is binomial and regulated by 0<mcof<1, mcof>0.5 for more males
mcof<-0.53

##Ages distribution controlled by age_stats table. Each row is a cohort with minimum 
##age as 'low', highest age as 'high', and share in the population as share. E.g. first
##row corresponds to people between 0 and 12 years old with share 0 (assuming we have
##adult oriented service)
age_stats<-data.frame(low=c(0, 12, 18, 24, 30, 36, 42, 48, 54), 
                      high=c(12, 18, 24, 30, 36, 42, 48, 54, 100), 
                      share=c(0, 0.10, 0.25, 0.30, 0.15, 0.12, 0.04, 0.03, 0.01))

##Email providers used by customers are fixed to email_stats table. 
##email_stats includes providers list as 'email' and their popularity as 'share'
email_stats<-data.frame(email=c("gmail.com", "yahoo.com", "outlook.com", "aol.com", "icloud.com"), 
                        share=c(53, 18, 14, 8, 2))

##ROW GENERATOR FUNCTION SECTION

CustomerGen<- function(numrows) {

##Declaring customers table as data.frame with data types
customers<-data.frame(first_name = "John",
                      last_name= "DOE", 
                      gender= "M",
                      dob= as.Date(Sys.Date()),
                      phone= "+19999999999",
                      email= "john.doe@hotmail.com",
                      ZIP= "00000",
                      city= "Seatle",
                      state= "WA",
                      stringsAsFactors = FALSE)

for (i in 1:numrows) {
        ##Gender and names generator.
        ##LOGIC: binomial distr with 'mcof' as male probability. Names are selected 
        ##from Male and Female names lists accordingly 
        if (rbinom(1,1,mcof)==1) {
                customers[i,1]<-tolower(toString(sample(mnames[,1], 1, replace = TRUE, prob = mnames[,2])))
                customers[i,2]<-tolower(toString(sample(snames[,1], 1, replace = TRUE, prob = snames[,2])))
                customers[i,3]<-"M"
        }
        else {
                customers[i,1]<-tolower(toString(sample(fnames[,1], 1, replace = TRUE, prob = fnames[,2])))
                customers[i,2]<-tolower(toString(sample(snames[,1], 1, replace = TRUE, prob = snames[,2])))    
                customers[i,3]<-"F"
        }
        ##DOB and age generator
        ##LOGIC:
        age_grp<-sample(nrow(age_stats), 1, replace=TRUE, prob=age_stats[,3])
        age<-round(age_stats$low[age_grp]+runif(1)*(age_stats$high[age_grp]-age_stats$low[age_grp]))
        customers$dob[i]<-as.Date(Sys.Date()-age*365+(runif(1,min = -1, max = 1))*365)
        ##Phone generator
        customers$phone[i]<-paste("+1",toString(round(runif(1,min = 0, max = 9)*1000000000)),sep = "")
        ##email generator
        email_type<-round(runif(1,min = 0.5, max = 4.5))
        if (email_type <=2) {
                email<-paste(customers[i,1],".", customers[i,2],"@",
                             sample(email_stats[,1], 1, replace = TRUE, prob = email_stats[,2]), sep = "")
        }
        else {
                email<-paste(substr(customers[i,1],1,1), customers[i,2],substr(customers[i,4],3,4),"@",
                             sample(email_stats[,1], 1, replace = TRUE, prob = email_stats[,2]), sep = "")
        }
        customers$email[i]<-email
        ##ZIP,city,state,country generator
        addr<-zcodes[sample(nrow(zcodes), 1, replace = TRUE, prob = zcodes$population),]
        customers$ZIP[i]<-addr$zip[1]
        customers$city[i]<-as.character(addr$city[1])
        customers$state[i]<-as.character(addr$state_id[1])
}
##email uniquness check/fix
counter<-0
while (counter<100|length(which(duplicated(customers$email)))!=0) {
        dups<-which(duplicated(customers$email))
        for (i in 1:length(dups)) {
                nr<-dups[i]
                customers$email[nr]<-paste(
                        substr(customers$email[nr],1,regexpr("@", customers$email[nr])[1]-1),
                        substr(customers$dob[nr],6,7),
                        substr(customers$dob[nr],9,10),
                        "@",
                        sample(email_stats[,1], 1, replace = TRUE, prob = email_stats[,2]), sep = "")
        }
        counter<-counter+1
}

return(customers)
}

