from bs4 import BeautifulSoup as soup  # HTML data structure
from urllib.request import urlopen as uReq  # Web client
import pandas as pd 
import numpy as np 
import sys
import time
import os

import requests
from stem import Signal
from stem.control import Controller
from fake_useragent import UserAgent 
import random
#pip install BeautifulSoup, pandas, numpy
#pip install openpyxl 
headers = {"User-Agent": UserAgent().random}
########netstat -an
proxies = {
    'http': 'socks5://127.0.0.1:9150',
    'https': 'socks5://127.0.0.1:9150'
}
def get_tor_session():
    session = requests.session()
    # Tor uses the 9050 port as the default socks port
    session.proxies = {'http':  'socks5://127.0.0.1:9150',
                     'https': 'socks5://127.0.0.1:9150'}
    return session 
def renew_session():
    with Controller.from_port(port = 9051) as c:
        c.authenticate()
        c.signal(Signal.NEWNYM)
# session = get_tor_session()
renew_session()
session =get_tor_session()
def clean_phone(raw_text):
    try:
        stripped_text = raw_text.strip()
        first_index = stripped_text.find("+")
        clean_text = stripped_text[first_index:]
        return clean_text
    except:
        print("Error in raw text")
        print(raw_text)
        print("Errors", sys.exc_info())

        return "None"
def clean_email(raw_text):
    try:
        stripped_text = raw_text.strip()
        ##w3schools.com/python/python_strings_slicing.asp
        first_index = stripped_text.find(" ")+2
        clean_text = stripped_text[first_index:]
        return clean_text
    except:
        print("Error in Raw Text")
        print(raw_text)
        print("Errors", sys.exc_info())

        return "None"
def write_webscrape_data(webscrape_dataframe = pd.DataFrame(), webscrape_dataframe_sorted = pd.DataFrame(), file_name = "default_name", sheet_name= "default_sheet_name"):
    try:
        with pd.ExcelWriter(file_name+".xlsx", mode = 'a', engine = "openpyxl") as writer:
            webscrape_dataframe.to_excel(writer, sheet_name=sheet_name)
        with pd.ExcelWriter(file_name+"_sorted.xlsx", mode = "a", engine = "openpyxl") as w:
            webscrape_dataframe_sorted.to_excel(w, sheet_name=sheet_name)
    except (FileNotFoundError, IOError):
        print("filenot found errooorrr")
        with pd.ExcelWriter(file_name+".xlsx", mode = 'w', engine = "openpyxl") as writer:
            webscrape_dataframe.to_excel(writer, sheet_name=sheet_name)
        with pd.ExcelWriter(file_name+"_sorted.xlsx", mode = "w", engine = "openpyxl") as w:
            webscrape_dataframe_sorted.to_excel(w, sheet_name=sheet_name)


col_names_webscrape = ["company_name_clean", "street_clean", "postalcode_clean",
"loc_info","company_phone_clean",
"company_email_clean","info_raw","company_name_raw","company_loc_raw",
"company_loc2_raw","company_email_raw","company_phone_raw"]

page_url = "https://www.wlw.de/de/suche?q=logistik&supplierTypes=Dienstleister"
https_start_links = "https://www.wlw.de"


def get_links(start_url, limit_number):
    ####Deciding limit number
    # eg For 2000 companies
    # each page has 30 company links
    # Number of pages required 2000/30==66.666 = 67
    # Limit number = 67

    # r =  requests.get(url2 , proxies = proxies, headers = headers)
    # r = session.get()

    main_link_list = [] #Site navigation
    link_list = [] #Collects the companylinks
    main_link_list.append(start_url)
    ####### main_link_list = [start_url,]
    link_template = {}
    total_links_list = [] # Help Write 
    #range(start,stop)

    for i in range(1,limit_number+1):
        ## Eg limit_number == 67
        #range == 1 to 68
        try:
            print("i = "+ str(i))
            # uClient  = uReq(main_link_list[i - 1])
            r = session.get(main_link_list[i - 1], proxies = proxies, headers = headers)
            # page_soup = soup(uClient.read(), "html.parser")
            page_soup = soup(r.content, "html.parser")

            # uClient.close()
            company_links = page_soup.find_all("div", {"class": "company-title-link-wrap"})
            for company_link in company_links:
                link_list.append(https_start_links+company_link.find("a", class_ = "company-title-link")["href"])
            pagination_class = page_soup.find("div", class_="pagination-next")
            print("pagination_class")
            print(pagination_class.div.find("a")["href"])
            main_link_list.append(https_start_links+pagination_class.div.find("a")["href"])
            # example_dict = {"key":data}
            total_links_list.append({"link":https_start_links+pagination_class.div.find("a")["href"]})
            wait = random.uniform(0,20)
            time.sleep(wait)
        except:
            print("link : "+ main_link_list[i-1]+ " was not valid")
            print("Errors in scarping for the links :", sys.exc_info())
            continue

    link_dataframe =  pd.DataFrame(total_links_list,columns = ["link"],
        index = pd.RangeIndex(0, len(total_links_list)))

        ##list_example = [1,2,3,4]
        #len(list_example) == 4
        # inidices will start from 0 to 3

    write_webscrape_data(link_dataframe, link_dataframe, "total_links_file", "link_sheet")
    print("main_link_list of length: " + str(len(main_link_list)))
    print(main_link_list)
    print("link_list of length: "+ str(len(link_list)))
    print(link_list)
    print("==========================================================================================]=============================")
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    return link_list
def get_webscrape_data(link_list):
    company_data_list = []
    total_company_data_list =[]
    count = 0

    for comp_link in link_list:
        try:
            count = count+1
            print("Number of Links to be scraped for individual data is : "+ str(len(link_list)))
            # uClient2 = uReq(comp_link)
            r2 = session.get(comp_link, proxies = proxies, headers = headers)

            # page_soup2 = soup(uClient2.read(), "html.parser")
            page_soup2 = soup(r2.content, "html.parser")

            # uClient2.close()
            info_container = page_soup2.find("div", class_ = "location-and-contact__left-box")
            company_template = {}
            # Company: -name, -street, postalcode, email-address
            company_template["company_name_raw"] = page_soup2.find("h1", class_ = "business-card__title").text #Raw
            company_template["company_loc_raw"] = page_soup2.find("span", id = "business-card__address")#raw
            company_template["company_loc2_raw"] = page_soup2.find("span", class_ = "business-card__address--link-inverse") #raw
            # raw_phone_data = fet
            # clean_phone(fetch)
            company_template["company_phone_clean"] = clean_phone(page_soup2.find("span", class_ = "phone-button__text").text)#clean
            company_template["company_phone_raw"] = page_soup2.find("span", class_ = "phone-button__text").text#raw
           
            company_template["company_email_clean"] = clean_email(page_soup2.find("a", id = "location-and-contact__email").text)#clean

            company_template["info_raw"] = info_container.find("address", class_ = "location-and-contact__address")#raw
            company_template["company_name_clean"] = company_template["info_raw"].find("strong").text
            company_temp2_rest = company_template["info_raw"].find_all("div")
            company_template["loc_info"] = company_temp2_rest[0].text
            company_template["street_clean"] = company_temp2_rest[1].text
            company_template["postalcode_clean"] = company_temp2_rest[2].text

            company_template["company_email_raw"] = info_container.find("a", id = "location-and-contact__email").text
            company_data_list.append(company_template)
            total_company_data_list.append(company_template)
            print(company_template)
            print("remaining ==== " + str(len(link_list ) - (count)))
            if count % 30 == 0 and count >= 30:
                webscrape_dataframe =  pd.DataFrame(company_data_list,columns = col_names_webscrape,index = pd.RangeIndex(0, len(company_data_list)))
                
                print("========================Time to write======================================")
                print(webscrape_dataframe["company_name_clean"].head())
                print(webscrape_dataframe["street_clean"].head())
                print(webscrape_dataframe["postalcode_clean"].head())
                print(webscrape_dataframe["company_phone_clean"].head())
                print(webscrape_dataframe["company_email_clean"].head())
                print(webscrape_dataframe["loc_info"].head())
                webscrape_dataframe_sorted = webscrape_dataframe[["company_name_clean","street_clean","postalcode_clean","company_phone_clean","company_email_clean", "loc_info"]]
                sheet_name = "scrape_no_"
                # sheet_name = os.path.expanduser("~/Desktop/web_scrape4/"+"scrape_no_"+ str((count//30)))
                write_webscrape_data(webscrape_dataframe,webscrape_dataframe_sorted, "webscrape_dataframe", sheet_name)
                # write_webscrape_data(webscrape_dataframe,webscrape_dataframe_sorted, os.path.expanduser("~/Desktop/web_scrape4/"+"webscrape_dataframe"+ str((count//30))), sheet_name)
                ###Path finder lesson learnt from here
                if count == len(link_list)-1:
                    total_dataframe = pd.DataFrame(total_company_data_list, columns = col_names_webscrape, index= pd.RangeIndex(0, len(total_company_data_list)))
                    total_dataframe_sorted = total_dataframe[["company_name_clean","street_clean","postalcode_clean","company_phone_clean","company_email_clean", "loc_info"]]
                    print("======================================= Writing Total Data=============")
                    write_webscrape_data(total_dataframe,total_dataframe_sorted, "webscrape_dataframe", sheet_name)
                company_data_list = []
                wait = random.uniform(0, 60)
                time.sleep(wait)
            else:
                wait =random.uniform(0, 60)
                time.sleep(wait)
            print("On to next ------------------------->>>>>")        
        except:
            print("Errors in individual page scraping", sys.exc_info())
            wait =  random.uniform(0, 60)
            time.sleep(wait)

            continue

    
## Pass the number of pages to be scraped eg 3
link_list = get_links(page_url, 90)
get_webscrape_data(link_list)

