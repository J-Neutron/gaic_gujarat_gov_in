from asyncio.windows_events import NULL
from calendar import c
from cgi import print_environ
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
import urllib.request


app_name = 'gaic_gujarat_gov_in'
all_file_path = f'C:\python\projects\{app_name}'
sqlite_path = f'{all_file_path}\{app_name}.db'
csv_path = f'{all_file_path}\{app_name}.csv'
download_path = os.path.expanduser('~') + '\\Documents\\pythonfiles\\' + app_name + '\\files'
main_data_list, link_list = [], []

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
# prefs = {'download.default_directory': all_file_path}
options.add_argument("user-data-dir=C:\\Path")
# options.add_experimental_option('prefs', prefs)


options.add_experimental_option('prefs', {
"download.default_directory": all_file_path, #Change default directory for downloads
"download.prompt_for_download": False, #To auto download the file
"download.directory_upgrade": True,
"plugins.always_open_pdf_externally": True #It will not show PDF directly in chrome
})



driver = webdriver.Chrome(options=options, service=Service(executable_path=f"{all_file_path}\chromedriver.exe"))

driver.get(search_url)
driver.maximize_window()

conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

def date_creater(text_data):
    x = text_data.replace(',', '')
    y = x.split(' ')
    date_string = y[1] + ' ' + y[0] + ' ' + y[2]
    print(date_string)
    date_object = datetime.strptime(date_string, "%d %b %Y")
    dates = str(date_object).split(' ')[0]
    d = dates.split('-')
    final_date = d[2] + '/' + d[1] + '/' + d[0]
    return final_date

def sqlite(databsse_path,list_od_data):
    conn = sqlite3.connect(databsse_path)
    cur = conn.cursor()
    tou = tuple(list_od_data)
    try:
        q = '''INSERT INTO tenders(Tender_Notice_No,OpeningDate,Bid_Deadline_1,Tender_Summery) VALUES(?,?,?,?)'''
        cur.execute(q, tou)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f'insert data from web to db {str(e)}')

def scrape():

    Tender_Notice_No = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table/tbody/tr[1]/td[3]'))).text.strip()
    main_data_list.append(Tender_Notice_No)

    OpeningDate = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table/tbody/tr[2]/td[3]'))).text.strip()
    main_data_list.append(OpeningDate)

    Bid_Deadline_1 = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table/tbody/tr[3]/td[3]'))).text.strip()
    main_data_list.append(Bid_Deadline_1)

    Tender_Summery = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ContentPlaceHolder1_TenderDetail1_trDescription"]/td[3]'))).text.strip()
    main_data_list.append(Tender_Summery)

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
                    os.rename(all_file_path + '\\' + i, all_file_path + '\\' + 'sppp_' + name1 + '.pdf')
                    time.sleep(1)
            shutil.move(all_file_path + '\\' + 'sppp_' + name1 + '.pdf', download_path)

        else:
            for i in os.listdir(all_file_path):
                if i.endswith('.pdf'):
                    name1 = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
                    os.rename(all_file_path + '\\' + i, all_file_path + '\\' + 'sppp_' + name1 + '.pdf')
                    time.sleep(1)
            name = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
            p = download_path + '\\' + 'sppp_' + name + '.zip'
            with zipfile.ZipFile(p, mode='w') as zipf:
                for i in os.listdir(all_file_path):
                    if i.endswith('.pdf'):
                        zipf.write(all_file_path + '\\' + i)
                        # os.remove(i)
            # time.sleep(1)
            for i in os.listdir(all_file_path):
                if 'pdf' in str(i):
                    os.remove(i)
    else:
        time.sleep(1)
        down(li)

def download_pdf(link_li):
    name = datetime.datetime.strptime(str(datetime.datetime.now()), '%Y-%m-%d %H:%M:%S.%f').strftime('%d%m%Y_%H%M%S.%f')
    for i in link_li:
        driver.execute_script(f"window.open('{i}');")
        time.sleep(1)
    down(link_li)
    # if len(link_li) != 1:
    #     # for i in link_li:
    #     #     print('mul link')
    #     #     driver.execute_script(f"window.open('{i}');")
    #     #     time.sleep(1)
    #     down(link_li)
    # else:
    #     driver.execute_script(f"window.open('{link_li[0]}');")
    #     time.sleep(1)
    #     print('single link','\t',link_li[0])


def ms_server(database_name):
    try:
        con = sqlite3.connect(database_name)
        cur = con.cursor()
        cur.execute("SELECT flag FROM tenders WHERE flag = ?", (1,))
        data = cur.fetchone()
        cur.execute("SELECT Tender_Notice_No,OpeningDate,Bid_Deadline_1,Tender_Summery FROM tenders WHERE flag = ?", (1,))
        data2 = cur.fetchall()


        if data != None:
            with pyodbc.connect('DRIVER={SQL Server};SERVER=153TESERVER;DATABASE=CrawlingDB;UID=hrithik;PWD=hrithik@123') as conn:
                with conn.cursor() as cursor:
                    q = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{app_name}' AND xtype='U') CREATE TABLE {app_name}(Id INTEGER PRIMARY KEY IDENTITY(1,1)\
                             ,Tender_Notice_No TEXT\
                             ,Tender_Summery TEXT\
                             ,Tender_Details TEXT\
                             ,Bid_deadline_2 TEXT\
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
                    q = f"INSERT INTO {app_name}(Tender_Notice_No,OpeningDate,Bid_Deadline_1,Tender_Summery) VALUES(?,?,?,?)"
                    cursor.execute(q, data2[0])
                    print(f'Data inserted on server')

            sql1 = f'UPDATE tenders SET flag ={0} WHERE flag = {1};'
            cur.execute(sql1)
            con.commit()
            cur.close()
            con.close()
        else:
            print(f'Data already available in sqlite database')
    except Exception as e:
        print(str(e))


page_index = 0
v = 1
while v == 1:
    v = 2
    page_index += 1
    time.sleep(1)

    try:
        # a = driver.find_elements(By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/ul/li')
        # print(a)

        for p, i in enumerate(driver.find_elements(By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/ul/li')):
            main_data_list = []
            OpeningDate = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, f'//*[@id="content"]/div[1]/div/div[2]/ul/li[{p+1}]/p[2]'))).text.strip()
            Tender_Summery = WebDriverWait(driver, 200).until(EC.presence_of_element_located((By.XPATH, f'//*[@id="content"]/div[1]/div/div[2]/ul/li[{p+1}]/a'))).text.strip()
            l = []

            l.append(OpeningDate)
            l.append(Tender_Summery)

            print(l)
            sql = """  CREATE TABLE IF NOT EXISTS tenders(Id INTEGER PRIMARY KEY
                                                         ,Tender_Notice_No TEXT
                                                         ,Tender_Summery TEXT
                                                         ,Tender_Details TEXT
                                                         ,Bid_deadline_2 TEXT
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
            cursor.execute(sql)
            conn.commit()
            conn.close()


            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            cursor.execute("SELECT OpeningDate FROM tenders WHERE OpeningDate = ? and Tender_Summery = ?", (OpeningDate, Tender_Summery,))
            a = cursor.fetchone()

            if a is None:
                ele = WebDriverWait(driver, 200).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="content"]/div[1]/div/div[2]/ul/li[{p+1}]/a')))
                ActionChains(driver).key_down(Keys.CONTROL).click(ele).key_up(Keys.CONTROL).perform()
                # ele.click()
                time.sleep(1)

                window_after = driver.window_handles[1]
                driver.switch_to.window(window_name=window_after)

                lnks = driver.find_element(By.XPATH, '//*[@id="content"]/div[1]/div/div[2]/table').find_elements(By.TAG_NAME, "a")

                scrape()

                print(main_data_list,'\n')

                link_list = []
                for link in lnks:
                    link_list.append(link.get_attribute('href'))

                download_pdf(link_list)

                sqlite(sqlite_path, main_data_list)

                ms_server(sqlite_path)


                driver.close()
                window_before = driver.window_handles[0]
                driver.switch_to.window(window_name=window_before)
            else:
                print('data already available', '\n')
        x = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, f"//*[@class='paginate_button current']")))
        print(f'--------------------- {x.text} end --------------------')
        ele1 = WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="examplesearch_next"]')))
        ele1.click()

    except StaleElementReferenceException as stale:  # We face this stale element reference exception when the element we are interacting with is destroyed and then recreated again. When this happens the reference of the element in the DOM becomes stale. Hence we are not able to get the reference to the element.
        print(f'StaleElementReferenceException {str(stale)}')
        driver.refresh()
    except TimeoutException as te:  # It should be Driver.close(). The TimeoutException will be thrown when the page is not loaded within specific time.
        print(f'TimeoutException {str(te)}')
        driver.close()
    except NoSuchElementException as ns:  # When we try to find any element in an HTML page that does not exist, NoSuchElementException will be raised.
        print(f'NoSuchElementException {str(ns)}')
        driver.close()
    except Exception as e:
        print(f'Main Exception hrithik {e}')
        driver.quit()
        break