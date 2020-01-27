##Tool for web crawling
library(Rcrawler)

##This is amazon best sellers home page we will extract 'ceed' urls here
starturl<-"https://www.amazon.com/Best-Sellers/zgbs/"

##Get seed URLs: all urls from start page with Best-Sellers in addr -> categories
##best sellers pages
ceedurls<-LinkExtractor(url = starturl, urlregexfilter = "/Best-Sellers")
ceedurls<-ceedurls$InternalLinks

##PREFACE
##2 stage process: First crawling around best sellers to generate products ASIN list.
##Second, generating product web page based on ASIN and scrapping out product name,
##category and other metadata.
##For stability this code involves frequent "save to disk" operations.

##ProdCrawler fuction takes list of 'ceed' urls, for each URL: crawls all ASIN 
##containing adresses (reg exp filter "/B0[[:alnum:]]{8}"), then goes to to each 
##found addr and gets all ASIN-containing URLs there (alternatives to best sellers)
##Dedup is perforned on every stage. 
##Full 'ceed' list crawling will take ~1.5h. for each ceed URL a file with ASINs
##list is created. Next Collector fuction is designed to consolidate them and dedup.
ProdCrawler <- function(urls, starturl=1, stopurl=length(urls)) {
        proddb<-NULL
        for (i in starturl:stopurl) {
                proddb1<-NULL
                proddb3<-NULL
                data1<-LinkExtractor(
                        url = urls[i], 
                        urlregexfilter = "/B0[[:alnum:]]{8}"
                        )
                m<-gregexpr(pattern = "/B0[[:alnum:]]{8}", data1[["InternalLinks"]])
                k<-regmatches(data1[["InternalLinks"]], m)
                proddb1<-c(proddb1,k)
                proddb1<-unique(proddb1)
                proddb2<-NULL
                if (length(proddb1)==0)
                {}
                else {
                for (n in 1:length(proddb1)) {
                        data2<-LinkExtractor(
                                paste0("https://www.amazon.com/dp",proddb1[[n]]),
                                urlregexfilter = "/B0[[:alnum:]]{8}"
                                )
                        m<-gregexpr(pattern = "/B0[[:alnum:]]{8}", data2[["InternalLinks"]])
                        k<-regmatches(data2[["InternalLinks"]], m)
                        proddb2<-c(proddb2,k)
                        Sys.sleep(0.5) #maybe not required
                        
                }}
                proddb2<-unique(proddb2)
                proddb3<-c(proddb1,proddb2)
                proddb3<-unique(proddb3)
                proddb3<-data.frame(
                        matrix(unlist(proddb3), nrow=length(proddb3), byrow=T),
                        stringsAsFactors = FALSE
                        )
                write.csv(proddb3, file = paste0("productdb", i,".csv"))
                ##proddb<-c(proddb,proddb3)
                Sys.sleep(2) #maybe not required
                print(i)
        }
        ##return(proddb)
}
##Collector fuction consolidates results of ProdCrawler in a single deduped 
##ASIN DB in a format with leading slash /
Collector<-function(filenum) {
        data1<-NULL
        data2<-NULL
        counter<-0
        for (i in 1:filenum) {
                data1<-read.csv(file = paste0("productdb", i,".csv"))
                data1<-as.vector(data1[,2])
                counter<-counter+length(data1)
                data2<-c(data2,data1)
                data2<-unique(data2)
                print(paste(length(data2), counter))
        }
        return(data2)
}

##ProdScrapper uses result of Collector function to construct product page url
##and gets out product data: category, name, sales rank in category, prices, 
##vendor and products details section. This funcion makes savepoint to disk for 
##each 100 products, total runtime for 9k products was ~8h.
ProdScraper <- function(prodslist) {
        frame1<-NULL
        data1<-NULL
        framei<-NULL
                for (i in 1:length(prodslist)) {
                urlnow<-paste0("https://www.amazon.com/dp",prodslist[[i]])
                data1<-ContentScraper(urlnow, 
                                      CssPatterns =c("div.a-subheader ul.a-unordered-list",
                                                     "span.a-size-large",
                                                     "#SalesRank",
                                                     ".a-text-strike",
                                                     "#priceblock_ourprice",
                                                     "#bylineInfo",
                                                     "#detail-bullets")
                                      )
                framei<-data.frame(do.call("cbind", data1), prodslist[[i]], stringsAsFactors = FALSE)
                if (ncol(framei)==8){
                colnames(framei)[8]<-"X8"
                frame1<-rbind(frame1, framei)
                }
                if (i%%100==0){ 
                        #savepoint block
                        write.csv(frame1, file = paste0("prodtable", i,".csv"))
                }
        }
        return(frame1)
}


##DATA PREPARATION (WRANGLING) down from here!!!!################################
#################################################################################
#################################################################################
#################################################################################
rawdata<- read.csv("Datalab/FakeDataHub/Products/datasets/prodtablefinal.csv", 
                   stringsAsFactors = FALSE)
##dorp rownum
rawdata<-rawdata[,2:9]
##Give descriptive col names
colnames(rawdata) <- c("category_all", 
                       "prodname", 
                       "rank", 
                       "price1", 
                       "price2", 
                       "vendor", 
                       "infoblock",
                       "ASIN")
#View NA by column
sapply(rawdata, function(x) sum(is.na(x)))
#lets drop 416 rows with NA prod names and re-check
rawdata<-rawdata[which(!is.na(rawdata$category_all)),]
#lets drop 36 more without prodname
rawdata<-rawdata[which(!is.na(rawdata$prodname)),]

#lets clean char strings from spaces,/n,/r
#category_all
#remove /r/n and excessive spaces (over 3)
rawdata$category_all<- gsub("[\r\n]", "", rawdata$category_all)
rawdata$category_all<- gsub("[ ]{3,}", "", rawdata$category_all)
#prodname
rawdata$prodname<- gsub("[\r\n]", "", rawdata$prodname)
rawdata$prodname<- gsub("[ ]{3,}", "", rawdata$prodname)
#rank
#looking at data: we can extract rank and category using #{} in {} till [().#]
#gathering positions of #
k1<-gregexpr("[#]", rawdata$rank)
#how many prod categories in rank we have
max(unlist(lapply(k1, length), use.names = FALSE))
#okay 4. lets try to normalize rank column to 8 columns:rank1,prodcat1,....
k2<-strsplit(rawdata$rank, "[#]")
#first element of this list is useless, while the second is spoiled by some html

unlister<-function(list1) {
        ncol<-max(unlist(lapply(list1, length), use.names = FALSE))
        nrow<-length(list1)
        dframe<-NULL
        for (i in 1:ncol) {
                column<-NULL
                for (n in 1:nrow) {
                        valn <- list1[[n]][i]
                        column<-c(column,valn)
                }
                dframe<-cbind(dframe, column)
               
        }
        return(dframe)
}

k3<-unlister(k2)

cleaner1<-function(colmn) {
        k<-strsplit(colmn, ".zg_hrsr", fixed = TRUE)
        k<-unlister(k)
        k<-strsplit(k[,1], " (See", fixed = TRUE)
        k<-unlister(k)
        k<-strsplit(k[,1], "[[:space:]]in[[:space:]]")
        k<-unlister(k)
        return(k)
}
k4<-cleaner1(k3[,2])
rank1<-trimws(k4[,1])
prodcat1<-trimws(k4[,2])
k4<-cleaner1(k3[,3])
rank2<-trimws(k4[,1])
prodcat2<-trimws(k4[,2])
k4<-cleaner1(k3[,4])
rank3<-trimws(k4[,1])
prodcat3<-trimws(k4[,2])
k4<-cleaner1(k3[,5])
rank4<-trimws(k4[,1])
prodcat4<-trimws(k4[,2])

rawdata<-subset(rawdata, select = -c(rank))
rawdata<-cbind(rawdata,rank1,prodcat1,rank2,prodcat2,rank3,prodcat3,rank4,prodcat4, stringsAsFactors = FALSE)
rm(k1,k2,k3,k4,rank1,rank2,rank3,rank4,prodcat1,prodcat2,prodcat3,prodcat4)
#note rank1 column of chars not only numeric!
which(grepl("[[:alpha:]]", rawdata$rank1)==TRUE)
#some apps contain rank like "#1 Paid in Category", getting rid of Paid and Free:
rawdata$rank1<-trimws(gsub("paid","",rawdata$rank1,ignore.case = TRUE))
rawdata$rank1<-trimws(gsub("free","",rawdata$rank1,ignore.case = TRUE))
#now good!
which(grepl("[[:alpha:]]", rawdata$rank1)==TRUE)

#now lets get back to categories and separate them to colums: cat1,...catn
#fist need to find out how many subcats we have
test1<-rawdata[,1]
test1<- gsub("[\r\n]", "", test1)
test1<- gsub("[ ]{3,}", "", test1)
test1<-gregexpr("[›]", test1)
test1<-lapply(test1, length)
test1<-unlist(test1, use.names = FALSE)
table(cut(test1,breaks = c(0,1,2,3,4,5,6,7,8,9)))
rm(test1)
#okay we have up 7 subcats,
k1<-strsplit(rawdata$category_all, "[›]")
k1<-unlister(k1)
k1<-trimws(k1)
colnames(k1)<-c("cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7")
rawdata<-subset(rawdata, select = -c(category_all))
rawdata<-cbind(k1,rawdata)
rm(k1)
#removing leadind slash in ASIN column:
rawdata$ASIN<-gsub("/", "", rawdata$ASIN)
#let's check price1 for non $,.,num chars:
rawdata$price1<-trimws(rawdata$price1)
length(which((grepl("^[[:digit:].$]*$", rawdata$price1)==FALSE)&(is.na(rawdata$price1)==FALSE)))
#Ok. All good. now we can get rid of $ and switch column to numeric-integer
rawdata$price1<-gsub("[$]","",rawdata$price1)
rawdata$price1<-as.numeric(rawdata$price1)
#Let's check price2:
rawdata$price2<-trimws(rawdata$price2)
length(which((grepl("^[[:digit:].$]*$", rawdata$price2)==FALSE)&(is.na(rawdata$price2)==FALSE)))
#ok 1k found. This is because of range pricing ($1 - $3). Lets add space and "-"
length(which((grepl("^[[:digit:]. $-]*$", rawdata$price2)==FALSE)&(is.na(rawdata$price2)==FALSE)))
#Only 1 found. Let's kill the fomat outlier
rawdata<-rawdata[-which((grepl("^[[:digit:]. $-]*$", rawdata$price2)==FALSE)&(is.na(rawdata$price2)==FALSE)),]
#now let's normalize price2 to price_min and price_max column:
k1<-strsplit(rawdata$price2, "-")
k1<-unlister(k1)
k1<-trimws(k1)
k1<-gsub("[$]","",k1)
k1[,1]<-as.numeric(k1[,1])
k1[,2]<-as.numeric(k1[,2])
colnames(k1)<-c("price", "price_max")
rawdata<-subset(rawdata, select = -c(price2))
rawdata<-cbind(rawdata, k1, stringsAsFactors = FALSE)
rawdata$price<-as.numeric(rawdata$price)
rawdata$price_max<-as.numeric(rawdata$price_max)
rm(k1)
#Now lets cleanup vendor column
#first basic cleanup including by pre-fix
rawdata$vendor<- gsub("[\r\n\t]", "", rawdata$vendor)
rawdata$vendor<- gsub("[ ]{3,}", "", rawdata$vendor)
rawdata$vendor<- gsub("by", "", rawdata$vendor)
rawdata$vendor<- trimws(rawdata$vendor)
#lets look around number of words
test1<-gregexpr("[[:space:]]", rawdata$vendor)
test1<-lapply(test1, length)
test1<-unlist(test1, use.names = FALSE)
table(cut(test1,breaks = c(0,1,2,3,4,5,6,7,8,9,10,11,12,13,25,50,100,300)))
rm(test1)
#Okay we have some long names 7 of(5-7words) and ~150 (300words!). lets look
#books has long vendor names, lets shorten them to First Author
k1<-strsplit(rawdata$vendor, "(Author", fixed = TRUE)
k1<-unlister(k1)
k1<-trimws(k1)
rawdata$vendor<-k1[,1]
rm(k1)
#Time for a hard part - infoblock
#lets try to decompose it to words and than analyse their frequency
#first lets get rid of all spaces and controls
rawdata$infoblock<- gsub("[\r\n\t]", " ", rawdata$infoblock)
rawdata$infoblock<- gsub("[ ]{2,}", " ", rawdata$infoblock)
rawdata$infoblock<- gsub("[[:space:]]", " ", rawdata$infoblock)
#simple parser to make vector of words
wordlist<-strsplit(rawdata$infoblock, "[[:space:]]")
wordvec<-tolower(unlist(wordlist, use.names = FALSE))
test1<-as.data.frame(table(wordvec))
test1<-test1[which(test1$Freq>100),]
#View(test1[order(-test1$Freq),])
rm(wordlist,wordvec,test1)
#extracting weight
k1<-gregexpr("weight", tolower(rawdata$infoblock), fixed = TRUE)
k1<-unlister(k1)
for (i in 1:nrow(k1)) {
        if (is.na(k1[i,1])==FALSE) {
        if (k1[i,1]==-1) {
                k1[i,1]<-NA
        }
        }
}
k1<-substr(rawdata$infoblock, k1[,1], k1[,1]+30)
k1<-strsplit(k1, " ")
k1<-unlister(k1)
k1<-trimws(k1)
k1<-tolower(k1)
k1<-k1[,c(2,3)]
for (i in 1:nrow(k1)) {
        if (is.na(k1[i,2])==FALSE) {
                if (k1[i,2]!="pounds"&k1[i,2]!="ounces") {
                        k1[i,2]<-NA
                        k1[i,1]<-NA
                }
                else if (k1[i,2]=="ounces") {
                        k1[i,2]<-"pounds"
                        k1[i,1]<-(as.numeric(k1[i,1])/16)
                }
        }
}
rawdata<-cbind(rawdata, k1[,1], stringsAsFactors = FALSE)
colnames(rawdata)[ncol(rawdata)]<-"weight_lb"
rawdata$weight_lb<-as.numeric(rawdata$weight_lb)
rm(k1)
#trying dimensions
k1<-gregexpr("dimensions", tolower(rawdata$infoblock), fixed = TRUE)
k1<-unlister(k1)
for (i in 1:nrow(k1)) {
        if (is.na(k1[i,1])==FALSE) {
                if (k1[i,1]==-1) {
                        k1[i,1]<-NA
                }
        }
}
k1<-substr(rawdata$infoblock, k1[,1], k1[,1]+50)
k1<-strsplit(k1, " ")
k1<-unlister(k1)
k1<-trimws(k1)
k1<-tolower(k1)
k1<-k1[,c(2,4,6,7)]
for (i in 1:nrow(k1)) {
        if (is.na(k1[i,4])==FALSE) {
                if (k1[i,4]!="inches") {
                        k1[i,]<-NA
                        }
                else {
                        k1[i,1]<-as.numeric(k1[i,1])
                        k1[i,2]<-as.numeric(k1[i,2])
                        k1[i,3]<-as.numeric(k1[i,3])
                }
        }
}
rawdata<-cbind(rawdata, k1[,1:3], stringsAsFactors = FALSE)
colnames(rawdata)[(ncol(rawdata)-2):ncol(rawdata)]<-c("dim1_in","dim2_in","dim3_in")
rawdata$dim1_in<-as.numeric(rawdata$dim1_in)
rawdata$dim2_in<-as.numeric(rawdata$dim2_in)
rawdata$dim3_in<-as.numeric(rawdata$dim3_in)
rm(k1)
#now dropping infoblock
rawdata<-subset(rawdata, select = -c(infoblock))
#now lets set fake price for products with NA price
#the goal is to keep mean and sd in price for category
#looking for rows where we dont have any price in category
table(rawdata$cat1[which(!is.na(rawdata$price)==TRUE|
                        !is.na(rawdata$price1)==TRUE|
                        !is.na(rawdata$price_max)==TRUE)])
#lets drop this rows
k1<-as.data.frame(table(rawdata$cat1[which(!is.na(rawdata$price)==TRUE|
                                     !is.na(rawdata$price1)==TRUE|
                                     !is.na(rawdata$price_max)==TRUE)]))
k1<-subset(k1, Freq==0)
k1<-as.vector(k1[,1])
rawdata<-subset(rawdata, !is.element(cat1, k1))        
rm(k1)        
#Now let's fill price with price1 where possible
rawdata$price[which(is.na(rawdata$price)==TRUE)]<-rawdata$price1[which(is.na(rawdata$price)==TRUE)]

#Creating price generator based on category statistics
PriceGen <- function(dframe) {
        th<-8 #minimum threshold of goods with price in same category to be significant for stats
        #counter<-length(which(is.na(dframe$price)==TRUE))
        #iteration<-0
        for (i in 1:nrow(dframe)) {
                if (is.na(dframe$price[i])==TRUE) { #take NA price row i
                        #timer<-Sys.time()
                        k<-length(which(!is.na(dframe[i,1:7])==TRUE)) #how many cats we have
                        #LOGIC: we take a subset of all goods with same cat1. If there are
                        #more than threshold goods with price we save progres to savepoint
                        #and go one cat deeper (up to 7) if we are below threshold on deeper
                        #level we take a subset(cohort) from previous savepoint
                        cohort<-dframe[which(dframe$cat1==dframe$cat1[i]),]
                        savepnt1<-cohort #cat1
                        if(length(which(!is.na(cohort$price)==TRUE))>th&k>1) {
                                cohort<-cohort[which(cohort$cat2==dframe$cat2[i]),]
                                savepnt2<-cohort #cat2
                                if(length(which(!is.na(cohort$price)==TRUE))>th&k>2) {
                                        cohort<-cohort[which(cohort$cat3==dframe$cat3[i]),]
                                        savepnt1<-cohort #cat3
                                        if(length(which(!is.na(cohort$price)==TRUE))>th&k>3) {
                                                cohort<-cohort[which(cohort$cat4==dframe$cat4[i]),]
                                                savepnt2<-cohort #cat4
                                                if(length(which(!is.na(cohort$price)==TRUE))>th&k>4){
                                                        cohort<-cohort[which(cohort$cat5==dframe$cat5[i]),]
                                                        savepnt1<-cohort #cat5
                                                        if(length(which(!is.na(cohort$price)==TRUE))>th&k>5){
                                                                cohort<-cohort[which(cohort$cat6==dframe$cat6[i]),]
                                                                savepnt2<-cohort #cat6
                                                                if(length(which(!is.na(cohort$price)==TRUE))>th&k>6){
                                                                        cohort<-cohort[which(cohort$cat7==dframe$cat7[i]),]
                                                                        if(length(which(!is.na(cohort$price)==TRUE))>th){
                                                                                result<-cohort #7cat
                                                                        } else {
                                                                        result<-savepnt2} #6cat
                                                                } else {
                                                                result<-savepnt1} #5cat
                                                        } else {
                                                        result<-savepnt2} #4cat
                                                } else {
                                                result<-savepnt1} #3cat
                                        } else {
                                        result<-savepnt2} #2cat
                                } else {
                                result<-savepnt1} #1cat
                        } else {
                        result<-cohort} #cat1
                        #Subsetting all goods with price for generating stats
                        stat<-result[which(!is.na(result$price)==TRUE),]
                        #excluding min and max prices (outliers)
                        if (nrow(stat)>th) {
                                stat<-stat[-c(which.max(stat$price),which.min(stat$price)),]
                        }
                        #Subsetting goods for price generation
                        filler<-result[which(is.na(result$price)==TRUE),]
                        #changed<-0
                        for (p in 1:nrow(filler)) {
                                #Generating price for goods and joining back to dframe
                                genprice<-round(abs(rnorm(1, mean(stat$price), (max(stat$price)-min(stat$price))/2)),2)
                                dframe$price[which(dframe$ASIN==filler$ASIN[p])]<-genprice
                                #counter<-counter-1
                                #changed<-changed+1
                        }
                        #counter<-counter-nrow(filler)
                        #iteration<-iteration+1
                        #print(paste(iteration,i, changed, counter,Sys.time()-timer))
                }
        }        
        return(dframe)
}
#Apply!
rawdata<-PriceGen(rawdata)
#now lets make use of ranks. The idea here is to crate sales probability
#convert to numeric
rawdata$rank1<-as.numeric(gsub(",","",rawdata$rank1))
rawdata$rank2<-as.numeric(gsub(",","",rawdata$rank2))
rawdata$rank3<-as.numeric(gsub(",","",rawdata$rank3))
rawdata$rank4<-as.numeric(gsub(",","",rawdata$rank4))
#handy func to compare NA values
compareNA <- function(v1,v2) {
        # This function returns TRUE wherever elements are the same, including NA's,
        # and false everywhere else.
        same <- (v1 == v2)  |  (is.na(v1) & is.na(v2))
        same[is.na(same)] <- FALSE
        return(same)
}
#function to assign power of rank: more power if category relevant and large
catpower<-function(dframe,cat) {
        k<-0
        if (is.na(cat)==TRUE){
                k<-0
        } else if (compareNA(dframe$cat1,cat)) {
                k<-10
        } else if (compareNA(dframe$cat2,cat)) {
                k<-8
        } else if (compareNA(dframe$cat3,cat)) {
                k<-6
        } else if (compareNA(dframe$cat4,cat)) {
                k<-4
        } else if (compareNA(dframe$cat5,cat)) {
                k<-3
        } else if (compareNA(dframe$cat6,cat)) {
                k<-2
        } else if (compareNA(dframe$cat7,cat)) {
                k<-1
        }
        return(k)
}
#function to normalize rank
nrank<-function(rank) {
        if (rank>10&rank<100) {
                rank<-10+round(rank,-1)/10
        } 
        if (rank>=100&rank<1000) {
                rank<-20+round(rank,-2)/100
        } 
        if (rank>=1000) {
                rank<-30
        } 
        return(rank)
}
#function to get probability score out of rank
rater<-function(dframe) {
        rating<-0
        if (is.na(dframe$rank1)==FALSE) {
                pow<-catpower(dframe, dframe$prodcat1)
                rating<-pow*(1/nrank(dframe$rank1))
        } 
        if (is.na(dframe$rank2)==FALSE) {
                pow<-catpower(dframe, dframe$prodcat2)
                rating<-rating+pow*(1/nrank(dframe$rank2))
        }
        if (is.na(dframe$rank3)==FALSE) {
                pow<-catpower(dframe, dframe$prodcat3)
                rating<-rating+pow*(1/nrank(dframe$rank3))
        }
        if (is.na(dframe$rank4)==FALSE) {
                pow<-catpower(dframe, dframe$prodcat4)
                rating<-rating+pow*(1/nrank(dframe$rank4))
        }
        return(rating)
}
#Now lets build our rating vector
rating<-NULL
for (i in 1:nrow(rawdata)) {
        r<-rater(rawdata[i,])
        rating<-c(rating,r)
}
#for products without raiting lets assign 0,9 of minimum
rating[which(rating==0)]<-min(rating[which(rating!=0)])*0.9
rawdata<-cbind(rawdata, rating, stringsAsFactors = FALSE)
#Finally omiting columns we dont need any more
rawdata<-subset(rawdata, select = -c(price1,rank1,rank2,rank3,rank4,prodcat1,prodcat2,prodcat3,prodcat4,price_max))
#saving results
write.csv(rawdata, "Datalab/FakeDataHub/Products/readydata/prod_table.csv")