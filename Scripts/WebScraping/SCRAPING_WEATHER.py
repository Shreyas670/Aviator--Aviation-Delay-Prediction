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

cService = webdriver.ChromeService(executable_path="C:\\Users\\ShreyasJ\\Desktop\\Kipithon\\chromedriver.exe")
driver = webdriver.Chrome(service=cService,options=optionss)

file = pd.read_excel('Cityandcode.xlsx')
cities = file.iloc[:, 0].tolist()
city_codes = file.iloc[:, 1].tolist()
airports = file.iloc[:, 2].tolist()


for day_num in [3]:
    listOfDict = list()
    for indx,airport in enumerate(airports):
        updateurl = 'https://www.accuweather.com/en/in/'+ airport + '/' + str(city_codes[indx]) + '/hourly-weather-forecast/' + str(city_codes[indx]) + '?day=' + str(day_num)
        #'https://www.accuweather.com/en/in/kempegowda-international-airport/3185_poi/hourly-weather-forecast/3185_poi?day=2'
        driver.get(updateurl)
        time.sleep(5)
        soup = bs(driver.page_source, 'html.parser')
        blocks = soup.select('div[class="accordion-item hour"]')
        

        for block in blocks:
            try:
                dataDict = dict()
                dataDict['City'] = cities[indx]
                dataDict['Month'] = date.today().month
                dataDict['Year'] = date.today().year
                dataDict['Date'] = date.today().day + day_num -1
                dataDict['Hour'] = block.select_one('div[class="hourly-card-subcontaint"]').select_one('h2.date').text.strip()
                dataDict['Temperature'] = block.select_one('div[class="hourly-card-subcontaint"]').select_one('div[class="temp metric"]').text.strip()
                dataDict['Forecast'] = block.select_one('div[class="phrase"]').text.strip()
                try:
                    for p in block.find_all('p'):
                        p_text = p.get_text()
                        if "Wind Gusts" in p_text:
                            dataDict['Wind Gusts'] = p.select_one('span[class="value"]').text.strip()
                        elif "Air Quality" in p_text:
                            pass
                        elif "Humidity" in p_text:
                            dataDict['Humidity'] = p.select_one('span[class="value"]').text.strip()
                        elif "Indoor Humidity" in p_text:
                            pass
                        elif "Dew Point" in p_text:
                            pass
                        elif "Visibility" in p_text:
                            dataDict['Visibility'] = p.select_one('span[class="value"]').text.strip()
                        elif "Cloud Cover" in p_text:
                            dataDict['Cloud Cover'] = p.select_one('span[class="value"]').text.strip()
                        elif "Cloud Ceiling" in p_text:
                            dataDict['Cloud Ceiling'] = p.select_one('span[class="value"]').text.strip()
                        elif "RealFeel Shade" in p_text:
                            pass
                        elif "Max UV Index" in p_text:
                            pass
                        elif "Rain" in p_text:
                            pass
                        else:
                            dataDict['Wind'] = p.select_one('span[class="value"]').text.strip()
                except:
                    pass
                dataDict['Precipitation'] = block.select_one('div[class="precip"]').text.strip()
                listOfDict.append(dataDict)
            except:
                    pass

    json_file_name = "Weather-" + str(str(int(date.today().day + day_num -1)) +'-'+ str(date.today().month) +'-'+ str(date.today().year)) + ".json"
    with open(json_file_name, "w") as json_file:
        json.dump(listOfDict, json_file)
        
    s3_client.upload_file(json_file_name, bucket_name, 'weatherdata/{}'.format(json_file_name)) 
