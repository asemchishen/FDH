## This code generates buyers descision for binary classification (to buy or not)
## based on CUSTOMERS table in Oracle Database. Customers data csv may be found
## here https://github.com/asemchishen/FDH/tree/master/Customers/readydata (use 50k)

## Connect to Oracle
con_file<-read.csv("Datalab/con_file.csv")
drv <- dbDriver("Oracle")
con <- dbConnect(drv, username =con_file$user, password=con_file$pass,dbname = con_file$con_string)
## Setting timezones params (required for DB writes)
Sys.setenv(ORA_SDTZ="Europe/Moscow")
Sys.setenv(TZ="Europe/Moscow")

## Reading in US ZIP data (https://simplemaps.com/data/us-)
zcodes<-read.csv("Datalab/FakeDataHub/Customers/datasets/uszips.csv", stringsAsFactors = FALSE)

## Filling local list for random customer selector

rs <- dbSendQuery(con, "select cust_id from customers")
cust_id <- fetch(rs)
cust_id <- cust_id[,1]

## PARAMETERS OF BUYERS LOGIC
obs_number   <-3000   ##number of observations to generate
baseline_prob<-0.1    ##baseline prob of customer to buy
age_peak     <-40     ##max prob multiplier at this age
age_mult     <-2      ##age max prob multiplier
gen_diff     <-0.5    ##gender diff (add to 1 for female, substract from 1 for male)
lng_max_mult <-2      ##Geo longitude max prob multiplier
lng_min_mult <-0.5    ##Geo longitude min prob multiplier
lat_max_mult <-2      ##Geo latitude max prob multiplier
lat_min_mult <-0.5    ##Geo latitude min prob multiplier


observations<-sample(cust_id, obs_number, replace = FALSE) ## sampling customers WO replacement
result_db<-NULL ##creating object for generated result

k1<-length(observations)%/%1000 
k2<-length(observations)%%1000
for (n in 0:k1) {
        if (n==k1) {l1<-k2} else {l1<-1000}
        result_tmp<-NULL
        start<-n*1000+1
        stop <-n*1000+l1
        if (start<=stop) {
                rs <- dbSendQuery(con, 
                                  paste0(
                                          "select * from customers where cust_id in(",
                                          paste(observations[start:stop], collapse = ","),
                                          ")")
                )
                data_temp<-fetch(rs)
                for (i in 1:l1) {
                        data<-data_temp[i,]
                        age<-round(as.numeric(Sys.Date()-as.Date(data$DOB))/365, 0)
                        gender<-data$GENDER
                        zip<-data$ZIP
                        lat<-zcodes[which(zcodes$zip==zip),2]
                        lng<-zcodes[which(zcodes$zip==zip),3]
                        ## p1 is an age probability multiplier (parabolic law via points: (age_peak, age_mult) and (0,0))
                        p1<-(-age_mult/age_peak^2)*age^2+2*age_mult/age_peak*age
                        if (p1<=0.1) {p1<-0.1} ##getting rid of negaives
                        
                        ## p2 is gender multiplier
                        if (gender=="F") {p2<-1+gen_diff} else {p2<-(1-gen_diff)}
                        
                        ## p3 for lng. lng min mult at center of US. (-95*) and max at coasts NY(-74*),LA(-118*) (linear law)
                        if (lng>=-95) {p3<-(lng_max_mult-lng_min_mult)/(-74+95)*lng+(95/(-74+95)*(lng_max_mult-lng_min_mult))+lng_min_mult} 
                        else {p3<-(lng_max_mult-lng_min_mult)/(-118+95)*lng+(95/(-118+95)*(lng_max_mult-lng_min_mult))+lng_min_mult}
                        ## limiting multiplier for non-continental territories
                        if (p3>lng_max_mult) {p3<-lng_max_mult}
                        if (p3<lng_min_mult) {p3<-lng_min_mult}
                        
                        ## p4 for lat. linear from min at south (29*) to max at north (49*)
                        p4<-(lat_max_mult-lat_min_mult)/(49-29)*lat+(lat_min_mult-29*(lat_max_mult-lat_min_mult)/(49-29))
                        if (p4>lat_max_mult) {p4<-lat_max_mult}
                        if (p4<lat_min_mult) {p4<-lat_min_mult}
                        
                        ##counting prob
                        p<-baseline_prob*p1*p2*p3*p4
                        if (p>=0.95) {p<-0.95}
                        ##probing descision based on binomial distr
                        ins<-rbinom(1,1,p)
                        if (ins==1) {dec1<-"Y"} else {dec1<-"N"}
                        ## pushing result to DB (Create INS_SALES DDL first!)
                        ## CREATE TABLE ins_sales (
                        ##        cust_id         NUMBER,
                        ##        descision       CHAR(1),
                        ##        CONSTRAINT check_descision
                        ##        CHECK (descision IN ('Y', 'N'))
                        ## );
                        dec<-as.data.frame(cbind(data$CUST_ID,dec1))
                        result_db<-rbind(result_db, dec)

                }
        }
}
##Push back to DB
dbWriteTable(con, "INS_SALES", result_db, row.names = FALSE, overwrite = FALSE,
             append = TRUE, ora.number = TRUE, schema = NULL, date = TRUE)
##write.csv(result_db, "Datalab/ins_sales(3k).csv", row.names = F)