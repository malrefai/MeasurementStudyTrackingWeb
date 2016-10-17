import sqlite3
import pandas.io.sql as sql
from urllib.parse import urlparse

def getDomainName(url):
    return urlparse(url).netloc

def connectDB(databaseName):
    return sqlite3.connect(databaseName)

def closeDB(databaseName):
    databaseName.commit()
    databaseName.close()
    print ('\tClosed thirdParty database successfully.... GOOD BYE!')

def csvFirstHttpRequest(thirdPartyDB, destinationPath):
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(firstRequest) as numberFirstRequest 
                    from page left outer natural join firstPartyHttpRequest 
                    group by (pageID) order by numberFirstRequest DESC''', thirdPartyDB).to_csv('{}/numberFirstRequest.csv'.format(destinationPath))
    print ('\tExported numberFirstRequest.csv file successfully...')
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(distinct firstRequest) as numberFirstRequest 
                        from page left outer natural join firstPartyHttpRequest 
                        group by (pageID) order by numberFirstRequest DESC''', thirdPartyDB).to_csv('{}/distinctNumberFirstRequest.csv'.format(destinationPath))
    print ('\tExported distinctNumberFirstRequest.csv file successfully...')

def csvThirdHttpRequest(thirdPartyDB, destinationPath):
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(thirdRequest) as numberThirdRequest 
                    from page left outer natural join thirdPartyHttpRequest 
                    group by (pageID) order by numberThirdRequest DESC''', thirdPartyDB).to_csv('{}/numberThirdRequest.csv'.format(destinationPath))
    print ('\tExported numberThirdRequest.csv file successfully...')
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(distinct thirdRequest) as numberThirdRequest 
                        from page left outer natural join thirdPartyHttpRequest 
                        group by (pageID) order by numberThirdRequest DESC''', thirdPartyDB).to_csv('{}/distinctNumberThirdRequest.csv'.format(destinationPath))
    print ('\tExported distinctNumberThirdRequest.csv file successfully...')

def csvFirstCookie(thirdPartyDB, destinationPath):
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(firstCookie) as numberFirstCookie 
                    from page left outer natural join firstPartyCookie 
                    group by (pageID) order by numberFirstCookie DESC''', thirdPartyDB).to_csv('{}/numberFirstCookie.csv'.format(destinationPath))
    print ('\tExported numberFirstCookie.csv file successfully...')
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(distinct firstCookie) as numberFirstCookie 
                        from page left outer natural join firstPartyCookie 
                        group by (pageID) order by numberFirstCookie DESC''', thirdPartyDB).to_csv('{}/distinctNumberFirstCookie.csv'.format(destinationPath))
    print ('\tExported distinctNumberFirstRequest.csv file successfully...')

def csvThirdCookie(thirdPartyDB, destinationPath):
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(thirdCookie) as numberThirdCookie 
                    from page left outer natural join thirdPartyCookie 
                    group by (pageID) order by numberThirdCookie DESC''', thirdPartyDB).to_csv('{}/numberThirdCookie.csv'.format(destinationPath))
    print ('\tExported numberThirdCookie.csv file successfully...')
    sql.read_sql_query('''select  pageID as AlexaRank, url as URL, count(distinct thirdCookie) as numberThirdCookie 
                        from page left outer natural join thirdPartyCookie 
                        group by (pageID) order by numberThirdCookie DESC''', thirdPartyDB).to_csv('{}/distinctNumberThirdCookie.csv'.format(destinationPath))
    print ('\tExported distinctNumberThirdCookie.csv file successfully...')

def csvGraph(thirdPartyDB, destinationPath):
    thirdPartyDB.execute('''create temporary table TFR  (pageID INT NOT NULL, httpRequestDomain TEXT NOT NULL);''')
    thirdPartyDB.execute('''create temporary table TTR  (pageID INT NOT NULL, httpRequestDomain TEXT NOT NULL);''')
    thirdPartyDB.execute('''create temporary table TDRV (vID INT NOT NULL PRIMARY KEY, pageID INT NOT NULL, httpRequestDomain TEXT NOT NULL);''')
    
    cursor = thirdPartyDB.execute('''select distinct pageID,firstRequest from firstPartyHttpRequest''')
    for row in cursor :
        thirdPartyDB.execute('''insert into TFR (pageID, httpRequestDomain) values ({0},"{1}");'''.format(row[0], getDomainName(row[1]).lower()))

    cursor = thirdPartyDB.execute('''select distinct pageID,thirdRequest from thirdPartyHttpRequest''')
    for row in cursor :
        thirdPartyDB.execute('''insert into TTR (pageID, httpRequestDomain) values ({0},"{1}");'''.format(row[0], getDomainName(row[1]).lower()))


    thirdPartyDB.execute('''create temporary table TDR as select distinct * from(
                            select distinct * from (select distinct * from TFR) 
                            union 
                            select distinct * from (select distinct * from TTR))''')
 
    cursor = thirdPartyDB.execute('''select distinct * from TDR''')
    i=0
    for row in cursor:
        i+=1
        thirdPartyDB.execute('''insert into TDRV (vID, pageID, httpRequestDomain) 
                                values ({0}, {1}, "{2}");'''.format(i, row[0], row[1]))  

    sql.read_sql_query('''select distinct vID, httpRequestDomain, url as urlReferrer 
                            from page natural join TDRV''', thirdPartyDB).to_csv('{}/httpRequestDomainVertex.csv'.format(destinationPath))
    print ('\tExported httpRequestDomainVertex.csv file successfully...')


    thirdPartyDB.execute('''create temporary table TDRE as select distinct A.pageID, A.vID as vIDS, B.vID as vIDE 
                            from (TDRV A join TDRV B using (pageID)) 
                            where A.vID < B.vID''')


    sql.read_sql_query('''select distinct vIDS, vIDE, url as urlReferrer  
                          from (page natural join TDRE)''', thirdPartyDB).to_csv('{}/httpRequestDomainEdge.csv'.format(destinationPath))
    print ('\tExported httpRequestDomainEdge.csv file successfully...')
    

def main(thirdPartyDbPath, destinationPath):
    thirdPartyDB = connectDB(thirdPartyDbPath)
    if thirdPartyDB == None:
        print('\tUnable to connect database in {}'.format(thirdPartyDbPath))
        exit()
    else:
        print ('\tOpened thirdParty database successfully....')

    csvFirstHttpRequest(thirdPartyDB, destinationPath)
    csvThirdHttpRequest(thirdPartyDB, destinationPath)
    csvFirstCookie(thirdPartyDB, destinationPath)
    csvThirdCookie(thirdPartyDB, destinationPath)
    csvGraph(thirdPartyDB, destinationPath)

    closeDB(thirdPartyDB)


destinationPath  = './files' 
thirdPartyDbPath = './files/thirdParty.sqlite'
main(thirdPartyDbPath, destinationPath)
