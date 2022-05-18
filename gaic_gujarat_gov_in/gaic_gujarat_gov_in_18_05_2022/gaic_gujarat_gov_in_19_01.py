import requests
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import time
import datetime
import sqlite3
import pyodbc
import zipfile
import shutil
import logging


app_name = 'gaic_gujarat_gov_in'
# all_file_path =  f'D:\python\projects\{app_name}'
all_file_path = os.getcwd()
sqlite_path = f'{all_file_path}\{app_name}.db'
csv_path = f'{all_file_path}\{app_name}.csv'
download_path = os.path.expanduser('~') + '\\Documents\\pythonfiles\\' + app_name + '\\files'
main_data_list, link_list = [], []
conns = sqlite3.connect(sqlite_path)
cur = conns.cursor()
log_path = f'{all_file_path}\{app_name}.log'
if os.path.exists(log_path):
    logging.basicConfig(filename=log_path, format='%(asctime)s %(message)s', filemode='a', level=logging.INFO)
else:
    logging.basicConfig(filename=log_path, format='%(asctime)s %(message)s', filemode='w', level=logging.INFO)
if os.path.exists(all_file_path):
    pass
else:
    os.makedirs(all_file_path)
if os.path.exists(download_path):
    pass
else:
    os.makedirs(download_path)


search_url = 'https://gaic.gujarat.gov.in/tenders.htm'
options = webdriver.ChromeOptions()
options.add_argument("user-data-dir=C:\\Path")
options.add_experimental_option('prefs', {
"download.default_directory": all_file_path, #Change default directory for downloads
"download.prompt_for_download": False, #To auto download the file
"download.directory_upgrade": True,
"plugins.always_open_pdf_externally": True #It will not show PDF directly in chrome
})
# driver = webdriver.Chrome(options=options, service=Service(executable_path=f"{all_file_path}\chromedriver.exe"))
# driver.get(search_url)
# driver.maximize_window()
driver = webdriver.Chrome(options=options)
driver.get(search_url)
logging.info(f'Connection to {search_url}')
driver.maximize_window()

def date_creater(text_data):
    x = text_data.replace(',', '')
    y = x.split(' ')
    date_string = y[1] + ' ' + y[0] + ' ' + y[2]
    # print(date_string)
    date_object = datetime.datetime.strptime(date_string, "%d %B %Y")
    dates = str(date_object).split(' ')[0]
    d = dates.split('-')
    final_date = d[2] + '/' + d[1] + '/' + d[0]
    return final_date


def sqlite_code(list_od_data):
    tou = tuple(list_od_data)
    try:
        q = '''INSERT INTO tenders(Tender_Notice_No,OpeningDate,Bid_deadline_2,Tender_Summery,Documents_2) VALUES(?,?,?,?,?)'''
        cur.execute(q, tou)
        conns.commit()
        logging.info(f'Data inserted into sqlite database {tou}')
        print('data inserted into sqlite database')

        cur.execute("SELECT flag FROM tenders WHERE flag = ?", (1,))
        data = cur.fetchone()
        cur.execute("SELECT Tender_Notice_No,OpeningDate,Bid_deadline_2,Tender_Summery,Documents_2 FROM tenders WHERE flag = ?",(1,))
        data2 = cur.fetchall()

        if data != None:
            with pyodbc.connect('DRIVER={SQL Server};SERVER=153TESERVER;DATABASE=CrawlingDB;UID=hrithik;PWD=hrithik@123') as conn:
                with conn.cursor() as cursor:
                    q = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{app_name}' AND xtype='U') CREATE TABLE {app_name}(Id INTEGER PRIMARY KEY IDENTITY(1,1)\
                                     ,Tender_Notice_No TEXT\
                                     ,Tender_Summery TEXT\
                                     ,Tender_Details TEXT\
                                     ,Documents_2 TEXT\
                                     ,TenderListing_key TEXT\
                                     ,Notice_Type TEXT\
                                     ,Competition TEXT\
                                     ,Purchaser_Name TEXT\
                                     ,Pur_Add TEXT\
                                     ,Pur_State TEXT\
                                     ,Pur_City TEXT\
                                     ,Pur_Country TEXT\
                                     ,Pur_Email TEXT\
                                     ,Pur_URL TEXT\
                                     ,Bid_Deadline_1 TEXT\
                                     ,Bid_deadline_2 TEXT\
                                     ,Financier_Name TEXT\
                                     ,CPV TEXT\
                                     ,scannedImage TEXT\
                                     ,Documents_1 TEXT\
                                     ,Documents_3 TEXT\
                                     ,Documents_4 TEXT\
                                     ,Documents_5 TEXT\
                                     ,currency TEXT\
                                     ,actualvalue TEXT\
                                     ,TenderFor TEXT\
                                     ,TenderType TEXT\
                                     ,SiteName TEXT\
                                     ,createdOn TEXT\
                                     ,updateOn TEXT\
                                     ,Content TEXT\
                                     ,Content1 TEXT\
                                     ,Content2 TEXT\
                                     ,Content3 TEXT\
                                     ,DocFees TEXT\
                                     ,EMD TEXT\
                                     ,OpeningDate TEXT\
                                     ,Tender_No TEXT)"
                    cursor.execute(q)
                    q = f"INSERT INTO {app_name}(Tender_Notice_No,OpeningDate,Bid_deadline_2,Tender_Summery,Documents_2) VALUES(?,?,?,?,?)"
                    cursor.execute(q, data2[0])
                    logging.info(f'Data inserted into server {data2[0]}')
                    print(f'Data inserted on server')

            sql1 = f'UPDATE tenders SET flag ={0} WHERE flag = {1};'
            logging.info(f'Flag updated')
            cur.execute(sql1)
            conns.commit()
        else:
            print(f'Data already available in sqlite database')
            logging.info(f'Data already available in sqlite database = {data}')
    except Exception as e:
        print(f'insert data from web to db {str(e)}')
        logging.error(f'{str(e)}')


def scraping_code():
    l = []
    Tender_Notice_No = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table/tbody/tr[1]/td[3]'))).text.strip()
    main_data_list.append(Tender_Notice_No)

    OpeningDate = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table/tbody/tr[2]/td[3]'))).text.strip()
    main_data_list.append(date_creater(OpeningDate))
    l.append(date_creater(OpeningDate))

    Bid_Deadline_2 = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table/tbody/tr[3]/td[3]'))).text.strip()
    main_data_list.append(date_creater(Bid_Deadline_2))

    Tender_Summery = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ContentPlaceHolder1_TenderDetail1_trDescription"]/td[3]'))).text.strip()
    main_data_list.append(Tender_Summery)
    l.append(Tender_Summery)

    logging.info(f'Scrape data from tender = {l}')
    print(l)


def down(li):
    files = []
    for i in os.listdir(all_file_path):
        if i.endswith('.pdf'):
            files.append(i)
    print(len(files), '\t', len(li))
    if len(files) == len(li):
        if len(li) == 1 and len(files) == 1:
            for i in os.listdir(all_file_path):
                if i.endswith('.pdf'):
                    name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
                    os.rename(all_file_path + '\\' + i, all_file_path + '\\' + 'gaic_' + name1 + '.pdf')
                    time.sleep(1)
                    shutil.move(all_file_path + '\\' + 'gaic_' + name1 + '.pdf', download_path)
                    main_data_list.append(download_path + '\\' + 'gaic_' + name1 + '.pdf')
                    d_path = download_path + '\\' + 'gaic_' + name1 + '.pdf'
                    logging.info(f"File download completed = {d_path}")
        else:
            for i in os.listdir(all_file_path):
                if i.endswith('.pdf'):
                    name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
                    os.rename(all_file_path + '\\' + i, all_file_path + '\\' + 'gaic_' + name1 + '.pdf')
                    d_path = all_file_path + '\\' + 'gaic_' + name1 + '.pdf'
                    logging.info(f"File download completed = {d_path}")
                    time.sleep(1)
            name = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
            p = download_path + '\\' + 'gaic_' + name + '.zip'
            logging.info(f"Made zip completed = {p}")
            main_data_list.append(p)
            with zipfile.ZipFile(p, mode='w') as zipf:
                for i in os.listdir(all_file_path):
                    if i.endswith('.pdf'):
                        zipf.write(i)
                        # os.remove(i)
            # time.sleep(1)
            for i in os.listdir(all_file_path):
                if 'pdf' in str(i):
                    d_p = all_file_path + '\\' + i
                    logging.info(f"File removed = {d_p}")
                    os.remove(i)
    else:
        time.sleep(1)
        down(li)


def download_pdf(link_li):
    for i in link_li:
        driver.execute_script(f"window.open('{i.get_attribute('href')}');")
        time.sleep(1)
    down(link_li)


page_index = 0
v = 1
while True:
    v = 2
    page_index += 1
    time.sleep(1)

    try:
        for p, i in enumerate(driver.find_elements(By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/ul/li')):
            main_data_list = []
            OpeningDate = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, f'//*[@id="content"]/div[1]/div/div[2]/ul/li[{p+1}]/p[2]'))).text.strip()
            Tender_Summery = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, f'//*[@id="content"]/div[1]/div/div[2]/ul/li[{p+1}]/a'))).text.strip()
            l = []
            OpeningDate = date_creater(OpeningDate)
            l.append(OpeningDate)
            l.append(Tender_Summery)
            logging.info(f'Scrape data from main page = {l}')
            print(l)
            sql = """  CREATE TABLE IF NOT EXISTS tenders(Id INTEGER PRIMARY KEY
                                                         ,Tender_Notice_No TEXT
                                                         ,Tender_Summery TEXT
                                                         ,Tender_Details TEXT
                                                         ,Documents_2 TEXT
                                                         ,TenderListing_key TEXT
                                                         ,Notice_Type TEXT
                                                         ,Competition TEXT
                                                         ,Purchaser_Name TEXT
                                                         ,Pur_Add TEXT
                                                         ,Pur_State TEXT
                                                         ,Pur_City TEXT
                                                         ,Pur_Country TEXT
                                                         ,Pur_Email TEXT
                                                         ,Pur_URL TEXT
                                                         ,Bid_Deadline_1 TEXT
                                                         ,Bid_deadline_2 TEXT
                                                         ,Financier_Name TEXT
                                                         ,CPV TEXT
                                                         ,scannedImage TEXT
                                                         ,Documents_1 TEXT
                                                         ,Documents_3 TEXT
                                                         ,Documents_4 TEXT
                                                         ,Documents_5 TEXT
                                                         ,currency TEXT
                                                         ,actualvalue TEXT
                                                         ,TenderFor TEXT
                                                         ,TenderType TEXT
                                                         ,SiteName TEXT
                                                         ,createdOn TEXT
                                                         ,updateOn TEXT
                                                         ,Content TEXT
                                                         ,Content1 TEXT
                                                         ,Content2 TEXT
                                                         ,Content3 TEXT
                                                         ,DocFees TEXT
                                                         ,EMD TEXT
                                                         ,OpeningDate TEXT
                                                         ,Tender_No TEXT
                                                         ,flag INT NOT NULL DEFAULT 1);  """
            cur.execute(sql)
            conns.commit()

            cur.execute("SELECT Tender_Summery FROM tenders WHERE OpeningDate = ? and Tender_Summery = ?", (OpeningDate, Tender_Summery,))
            a = cur.fetchone()

            if a is None:
                ele = WebDriverWait(driver, 200).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="content"]/div[1]/div/div[2]/ul/li[{p+1}]/a')))
                ActionChains(driver).key_down(Keys.CONTROL).click(ele).key_up(Keys.CONTROL).perform()
                # ele.click()
                time.sleep(1)

                window_after = driver.window_handles[1]
                driver.switch_to.window(window_name=window_after)

                # driver.find_element(By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table').find_elements(By.TAG_NAME, "a")

                scraping_code()
                lnks = driver.find_element(By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table')
                links_li = WebDriverWait(lnks, 4).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
                # print(links_li)

                download_pdf(links_li)
                sqlite_code(main_data_list)

                driver.close()
                window_before = driver.window_handles[0]
                driver.switch_to.window(window_name=window_before)
            else:
                logging.info(f'Data already available in sqlite database {l}')
                print('data already available', '\n')
        # x = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, f"//*[@class='paginate_button current']")))
        # print(f'--------------------- {x.text} end --------------------')
        # ele1 = WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="examplesearch_next"]')))
        # ele1.click()
        conns.close()

    except StaleElementReferenceException as stale:  # We face this stale element reference exception when the element we are interacting with is destroyed and then recreated again. When this happens the reference of the element in the DOM becomes stale. Hence we are not able to get the reference to the element.
        print(f'StaleElementReferenceException {str(stale)}')
        logging.error(f'{str(stale)}')
        driver.refresh()
    except TimeoutException as te:  # It should be Driver.close(). The TimeoutException will be thrown when the page is not loaded within specific time.
        print(f'TimeoutException {str(te)}')
        logging.error(f'{str(te)}')
        driver.close()
    except NoSuchElementException as ns:  # When we try to find any element in an HTML page that does not exist, NoSuchElementException will be raised.
        print(f'NoSuchElementException {str(ns)}')
        logging.error(f'{str(ns)}')
        driver.close()
    except Exception as e:
        driver.close()
        logging.error(f'{str(e)}')
        print(f'Main Exception hrithik {e}')
    finally:
        logging.info(f'Driver Quit')
        driver.quit()
        break
