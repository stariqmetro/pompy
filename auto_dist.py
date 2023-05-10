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

def download_dist():
    #change download location
    DOWNLOAD_DIR = r"C:\OneDrive - Metropolitan Warehouse\Vendor Control\Data Files\POM Level\Downloads\Auto"
    PASSWORD_DIR = "C:/OneDrive - Metropolitan Warehouse/Vendor Control/Data Files/POM Level/Helpers/Password.txt"
    DIST_DIR = r'C:\OneDrive - Metropolitan Warehouse\Vendor Control\Data Files\POM Level\TMSProduct\temp_dist.csv'

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
    
    driver.get("https://pplus.metropolitanwarehouse.com/zip_distance.aspx")
    choose_file = driver.find_element(by=By.XPATH,
                                      value='//*[@id="ContentPlaceHolder1_FileUpload1"]')
    choose_file.send_keys(DIST_DIR)

    two_columns = driver.find_element(by=By.XPATH,
                                      value='//*[@id="ContentPlaceHolder1_rbTwo"]')
    two_columns.click()
    
    get_btn = driver.find_element(by=By.XPATH,
                                  value='//*[@id="ContentPlaceHolder1_Button1"]')
    get_btn.click()

    #wait for download to finish
    download_wait(DOWNLOAD_DIR)

    driver.quit()
    return
