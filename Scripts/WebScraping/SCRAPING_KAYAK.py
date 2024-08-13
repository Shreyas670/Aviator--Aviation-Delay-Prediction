from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import time
from bs4 import BeautifulSoup as bs
from selenium.common.exceptions import NoSuchElementException
import re, json
from datetime import date, timedelta, datetime
import boto3

aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = 'aviatorkipithon'
# Create a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Create an S3 client
s3_client = session.client('s3')

# TODO implement
optionss = webdriver.ChromeOptions() 
optionss.add_argument("start-maximized")
optionss.add_experimental_option("excludeSwitches", ["enable-automation"])
optionss.add_experimental_option('useAutomationExtension', False)
#driver = webdriver.Chrome(ChromeDriverManager().install(), options=optionss)

cService = webdriver.ChromeService(executable_path="D:\\kipithon\\chromedriver.exe")
driver = webdriver.Chrome(service=cService,options=optionss)

# Read url Excel file.
file = pd.read_excel('Cities.xlsx')
origin = file.iloc[:, 0].tolist()
destination = file.iloc[:, 1].tolist()


curr_date = "2024-08-04"

listOfDict = list()
rowcount = 1
for indx, ori in enumerate(origin):
    dest = destination[indx]
    updateurl = 'https://www.kayak.co.in/flights/' + ori + '-' + dest + '/' + curr_date + '?sort=bestflight_a'
    print(updateurl)
    driver.get(updateurl)
    time.sleep(5)
    soup = bs(driver.page_source, 'html.parser')
    while True:
        try:
                # sleep for 3 seconds to load page
            time.sleep(5)
                # click to show more results
            driver.find_element(By.XPATH, '//div[contains(text(), "Show more results")]').click()
        except NoSuchElementException:
            break

        # Page source Data
    soup = bs(driver.page_source, 'html.parser')
    blocks = soup.select('div[class="Fxw9"] div[class="nrc6-inner"]')

        # Extract all data of block
    for indx, block in enumerate(blocks):
        try:
            dataDict = dict()
            dataDict['ID'] = rowcount
            rowcount += 1
            dataDict['Date'] = curr_date
            dataDict['Airline'] = block.select_one('div[class="J0g6-operator-text"]').text.strip()
            dataDict['Source'] = ori
            dataDict['Destination'] = dest
            dataDict['Stops'] = block.select_one('span.JWEO-stops-text').text.strip()
            dataDict['Duration'] = block.select_one('div[class="xdW8 xdW8-mod-full-airport"]').find('div', string=re.compile('.*h.*')).text.strip()
            price_text = block.select_one('div[class="f8F1-price-text"]').text.strip()
            price_value = re.sub(r'\D', '', price_text)
            dataDict['Price'] = price_value.replace('₹', '')
            dataDict['Departure_Time'] = block.select('div[class="vmXl vmXl-mod-variant-large"]>span')[0].text.strip()
            dataDict['Arrival_Time'] = block.select('div[class="vmXl vmXl-mod-variant-large"]>span')[-1].text.strip()
            dataDict['Class'] = block.select_one('div[class="aC3z-name"]').text.strip()
            #print(dataDict)
            listOfDict.append(dataDict)
        except:
            dataDict = dict()
            dataDict['ID'] = rowcount
            rowcount += 1
            dataDict['Date'] = curr_date
            dataDict['Airline'] = ""
            dataDict['Source'] = ori
            dataDict['Destination'] = dest
            dataDict['Stops'] = ""
            dataDict['Duration'] = ""
            price_text = ""
            price_value = ""
            dataDict['Price'] = ""
            dataDict['Departure_Time'] = ""
            dataDict['Arrival_Time'] = ""
            dataDict['Class'] = ""
            #print(dataDict)
            listOfDict.append(dataDict)

# Save Data in JSON file
   
json_file_name = "KayakFlight_" + curr_date + ".json"
with open(json_file_name, "w") as json_file:
    json.dump(listOfDict, json_file)

s3_client.upload_file(json_file_name, bucket_name, 'flightsdata/{}'.format(json_file_name))
