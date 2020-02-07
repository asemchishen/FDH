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
rs <- dbSendQuery(con, "select max(ROWNUM) from customers")
maxrow <- fetch(rs)[1,1]
rs <- dbSendQuery(con, "select cust_id from customers")
cust_id <- fetch(rs)
cust_id <- cust_id[,1]
casedata<-NULL

## PARAMETERS OF BUYERS LOGIC
obs_number<-1500      ##number of observations to generate
baseline_prob<-0.1   ##baseline prob of customer to buy
peak_age<-40         ##max prob multiplier at this age
age_mult<-2          ##age max prob multiplier
gen_mult_add<-0.5    ##gender diff (add to 1 for female, substract from 1 for male)
lng_max_mult<-2      ##Geo longitude max prob multiplier
lng_min_mult<-0.5    ##Geo longitude min prob multiplier
lat_max_mult<-2      ##Geo latitude max prob multiplier
lat_min_mult<-0.5    ##Geo latitude min prob multiplier


observations<-sample(cust_id, obs_number, replace = FALSE) ## sampling customers WO replacement
for (i in 1:length(observations)) {
        rownum<-observations[i] ## customer from a list
        rs <- dbSendQuery(con, paste0("select * from customers where cust_id=",rownum))
        data<-fetch(rs) ## getting customer details
        ## fetching features
        age<-round(as.numeric(Sys.Date()-as.Date(data$DOB))/365, 0)
        gender<-data$GENDER
        zip<-data$ZIP
        lat<-zcodes[which(zcodes$zip==zip),2]
        lng<-zcodes[which(zcodes$zip==zip),3]
        ## p1 is an age probability multiplier (parabolic law via points: (peak_age, age_mult) and (0,0))
        p1<-(-age_mult/peak_age^2)*age^2+2*age_mult/peak_age*age
        if (p1<=0) {p1<-0.1} ##getting rid of negaives
        
        ## p2 is gender multiplier
        if (gender=="F") {p2<-1+gen_mult_add} else {p2<-(1-gen_mult_add)}
        
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
        dec<-as.data.frame(cbind(rownum,dec1))
        dbWriteTable(con, "INS_SALES", dec, row.names = FALSE, overwrite = FALSE,
                     append = TRUE, ora.number = TRUE, schema = NULL, date = TRUE)
        ## this is a force reconnect for each 450 rows to overcome open coursor issues
        if (i%%450==0) {
        con <- dbConnect(drv, username =con_file$user, password=con_file$pass,dbname = con_file$con_string)
        }
}


