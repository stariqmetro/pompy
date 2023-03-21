# -*- coding: utf-8 -*-
"""
Downloads Closing Tickets from Order# report for given orders.
Expects a string of manifests seperated by a '\n'.
"""
import os
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def download_wait(directory):
    """
    Args
    ----
    directory : str
        The path to the folder where the file will be downloaded.
    """
    dl_wait = True
    while dl_wait:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)

        for fname in files:
            if fname.endswith('.crdownload') or fname.endswith('.tmp'):
                dl_wait = True
    return

def download_closing_tickets():
    #change download location
    DOWNLOAD_DIR = r'C:\Users\shahmir.tariq\Downloads\Auto'
    PASSWORD_DIR = "../Helpers/Password.txt"

    with open(PASSWORD_DIR) as password_file:
        username = password_file.readline().strip('\n')
        password = password_file.readline()
    
    chrome_options = webdriver.ChromeOptions()
    prefs = {'download.default_directory' : DOWNLOAD_DIR}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://pplus.metropolitanwarehouse.com/")
    
    title = driver.title
    assert title == "PinnaclePlus Login"
    driver.implicitly_wait(0.5)
    
    text_box = driver.find_element(by=By.ID, value="TxtUserID") #username
    password_box = driver.find_element(by=By.ID, value="txtPassword")
    submit_button = driver.find_element(by=By.ID, value="btnLogin") #Log In
    
    text_box.send_keys(username)
    password_box.send_keys(password)
    submit_button.click()
    
    title = driver.title
    assert title == "PinnaclePlus"
    
    driver.get("https://pplus.metropolitanwarehouse.com/reports.aspx")
    search_box = driver.find_element(by=By.ID, value="ContentPlaceHolder1_txtSearch")
    search_box.send_keys("Closing Tickets From Order#")
    #wait until the text appears in the dropdown
    element = WebDriverWait(driver,10).until(EC.presence_of_element_located(
      (By.XPATH, "//*[contains(text(), 'Closing Tickets From Order#')]"))
    )
    element.click()
    
    order_box = driver.find_element(by=By.ID, value="ContentPlaceHolder1_gvData_txtPara_0")
    order_box.clear()
    order_box.send_keys(Keys.CONTROL, 'a'),
    order_box.send_keys(Keys.CONTROL, 'v')
    get_xl = driver.find_element(by=By.ID, value="ContentPlaceHolder1_btnXL")
    get_xl.click()

    #wait for download to finish
    download_wait(DOWNLOAD_DIR)

    driver.quit()
    return
