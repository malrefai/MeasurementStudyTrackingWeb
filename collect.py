import re
import time
import threading
import sqlite3
from shutil import copy
from selenium import webdriver
from selenium.webdriver.firefox.webdriver import FirefoxProfile
from urllib.parse import urlparse

def timer(threadURL, secs, url, num):
    totalSec =1
    # Loop until we reach t secs running
    while threadURL.isAlive():
        print ('{0:>3d}- {1} >>>>> {2}'.format(num, url, totalSec), end='\r')
        time.sleep(1)   # Sleep for a one sec
        totalSec += 1   # Increment the secs total
    
    doneSec = totalSec + secs

    while totalSec < doneSec :
        print ('{0:>3d}- {1} >>>>> {2}'.format(num, url, totalSec), end='\r')
        time.sleep(1)   # Sleep for a one sec
        totalSec += 1   # Increment the secs total

    print ('{0:>3d}- {1} >>>>> {2} seconds'.format(num, url, totalSec-1))
    return totalSec-1

def readURL(fileObject):
    tmp= fileObject.readline().split(',')
    url = 'http://www.{}'.format(tmp[1].strip())
    return url

def getDomainName(url):
    return urlparse(url).netloc

def connectDB(databaseName):
    return sqlite3.connect(databaseName)


def createPageTable(thirdPartyDB):
    thirdPartyDB.execute('''CREATE TABLE IF NOT EXISTS page(pageID INT PRIMARY KEY NOT NULL,
                                                            url TEXT NOT NULL,
                                                            waitSecs INT NOT NULL);''')
    thirdPartyDB.execute('''DELETE FROM page''')

def createFirstHttpRequestTable(thirdPartyDB):
    thirdPartyDB.execute('''CREATE TABLE IF NOT EXISTS firstPartyHttpRequest(pageID INT NOT NULL,
                                                                             firstRequest TEXT NOT NULL);''')
    thirdPartyDB.execute('''DELETE FROM firstPartyHttpRequest''')

def createThirdHttpRequestTable(thirdPartyDB):   
    thirdPartyDB.execute('''CREATE TABLE IF NOT EXISTS thirdPartyHttpRequest(pageID INT NOT NULL,
                                                                             thirdRequest TEXT NOT NULL);''')
    thirdPartyDB.execute('''DELETE FROM thirdPartyHttpRequest''')

def createFirstCookieTable(thirdPartyDB):       
    thirdPartyDB.execute('''CREATE TABLE IF NOT EXISTS firstPartyCookie(pageID INT NOT NULL,
                                                                        firstCookie TEXT NOT NULL);''')
    thirdPartyDB.execute('''DELETE FROM firstPartyCookie''')

def createThirdCookieTable(thirdPartyDB):        
    thirdPartyDB.execute('''CREATE TABLE IF NOT EXISTS thirdPartyCookie(pageID INT NOT NULL,
                                                                        thirdCookie TEXT NOT NULL);''')
    thirdPartyDB.execute('''DELETE FROM thirdPartyCookie''')

def creatThirdPartyDB(thirdPartyDbPath):
    thirdPartyDB = connectDB(thirdPartyDbPath)
    if thirdPartyDB == None:
        print('Unable to connect database in {}'.format(thirdPartyDbPath))
        exit()
    else:
        print ('Opened thirdParty database successfully....')

    createPageTable(thirdPartyDB)
    createFirstHttpRequestTable(thirdPartyDB)
    createThirdHttpRequestTable(thirdPartyDB)
    createFirstCookieTable(thirdPartyDB)
    createThirdCookieTable(thirdPartyDB)
    return thirdPartyDB

def insertValueIntoPage(thirdPartyDB, dataTuple):
    sqlSen = 'INSERT INTO page (pageID,url,waitSecs) VALUES ({0},"{1}",{2});'.format(dataTuple[0], dataTuple[1], dataTuple[2])
    thirdPartyDB.execute(sqlSen)

def insertValueIntoFirstHttpRequest(thirdPartyDB, fourthPartyDB, dataTuple):
    cursor = fourthPartyDB.execute('SELECT url from http_requests')
    urlReferrer = getDomainName(dataTuple[1]).lower()
    for row in cursor:
        urlRequest  = getDomainName(row[0]).lower()
        if not urlRequest:
            continue
        if urlRequest == urlReferrer:
            sqlSen = 'INSERT INTO firstPartyHttpRequest (pageID,firstRequest) VALUES ({0},"{1}");'.format(dataTuple[0], row[0].lower())
            thirdPartyDB.execute(sqlSen)
            

def insertValueIntoThirdHttpRequest(thirdPartyDB, fourthPartyDB, dataTuple):
    cursor = fourthPartyDB.execute('SELECT url from http_requests')
    urlReferrer = getDomainName(dataTuple[1]).lower()
    for row in cursor:
        urlRequest  = getDomainName(row[0]).lower()
        if not urlRequest:
            continue
        if urlRequest != urlReferrer:
            sqlSen = 'INSERT INTO thirdPartyHttpRequest (pageID,thirdRequest) VALUES ({0},"{1}");'.format(dataTuple[0], row[0].lower())
            thirdPartyDB.execute(sqlSen)

def insertValueIntoFirstCookie(thirdPartyDB, fourthPartyDB, dataTuple):
    cursor = fourthPartyDB.execute('SELECT raw_host from cookies')
    urlReferrer = getDomainName(dataTuple[1]).lower()
    for row in cursor:
        urlRequest  = row[0].lower()
        if not urlRequest:
            continue
        if not (re.search(R'^www\.[a-z0-9-.]+\.[a-z]{1,3}$',urlRequest,re.I)):
            urlRequest = 'www.{}'.format(urlRequest)
        if urlRequest == urlReferrer:
            sqlSen = 'INSERT INTO firstPartyCookie (pageID,firstCookie) VALUES ({0},"{1}");'.format(dataTuple[0], row[0].lower())
            thirdPartyDB.execute(sqlSen)

def insertValueIntoThirdCookie(thirdPartyDB, fourthPartyDB, dataTuple):
    cursor = fourthPartyDB.execute('SELECT raw_host from cookies')
    urlReferrer = getDomainName(dataTuple[1]).lower()
    for row in cursor:
        urlRequest  = row[0].lower()
        if not urlRequest:
            continue
        if not (re.search(R'^www\.[a-z0-9-.]+\.[a-z]{1,3}$',urlRequest,re.I)):
            urlRequest = 'www.{}'.format(urlRequest)
        if urlRequest != urlReferrer:
            sqlSen = 'INSERT INTO thirdPartyCookie (pageID,thirdCookie) VALUES ({0},"{1}");'.format(dataTuple[0], row[0].lower())
            thirdPartyDB.execute(sqlSen)

def insertIntoThirdPartyDB(thirdPartyDB, fourthPartyDB, dataTuple):
    insertValueIntoPage(thirdPartyDB, dataTuple)
    insertValueIntoFirstHttpRequest(thirdPartyDB, fourthPartyDB, dataTuple)
    insertValueIntoThirdHttpRequest(thirdPartyDB, fourthPartyDB, dataTuple)
    insertValueIntoFirstCookie(thirdPartyDB, fourthPartyDB, dataTuple)
    insertValueIntoThirdCookie(thirdPartyDB, fourthPartyDB, dataTuple)
    thirdPartyDB.commit()


def main(numURLs, secsWait, secsTimeOut, extensionPath, destinationPath, thirdPartyDbPath):

    thirdPartyDB = creatThirdPartyDB(thirdPartyDbPath)
    urls = open('{}/top-1m.csv'.format(destinationPath),"r")
    urls.seek(0,0)
    
    for i in range(numURLs):
        url = readURL(urls)
        #if not ((i+1) in [95, 143, 268, 275, 321, 338, 456]):continue
        # create a new Firefox session
        profile = webdriver.FirefoxProfile()
        profile.add_extension(extension=extensionPath)
        driver = webdriver.Firefox(profile)
        driver.set_page_load_timeout(secsTimeOut)
        driver.maximize_window()


        profiletmp = driver.firefox_profile.path
        fourthPartyDbPath = profiletmp+'/fourthparty.sqlite'
        #cookiesSqlite= profiletmp+'/cookies.sqlite'
 
        t = threading.Thread(target=driver.get, args=(url,))    # navigate to url home page
        t.start()
        secTime = timer(t, secsWait, url, i+1)

        #print ("Saving the files ...........")
        #copy(fourthPartyDbPath, destinationPath)
        #copy(cookiesSqlite, destinationPath)

        fourthPartyDB = connectDB(fourthPartyDbPath)
        dataTuple = (i+1, url, secTime)
        insertIntoThirdPartyDB(thirdPartyDB, fourthPartyDB, dataTuple)

        # close the browser window
        driver.quit()

        # close fourthParty database
        fourthPartyDB.close()

        #t.join()
       
     

    # close database 
    thirdPartyDB.close()

    # close urls file
    urls.close()



extensionPath    = './fourthparty/extension/fourthparty-jetpack.1.13.2.xpi'
destinationPath  = './files' 
thirdPartyDbPath = './files/thirdParty.sqlite'
main(500, 30, 120, extensionPath, destinationPath, thirdPartyDbPath)
