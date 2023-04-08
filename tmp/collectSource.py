import json
import os
import sys
import pandas as pd
import numpy as np
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
import csv


def writeSource(path, data):
    with open(path, 'a+', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False)
        f.write('\n')


def collectLink(driver, path):
    url = ytb_url
    driver.implicitly_wait(10)
    driver.get(url)

    # Login spotify with email and password

    category = driver.find_elements(By.CSS_SELECTOR, '#chips > yt-chip-cloud-chip-renderer')
    for ele in category:
        ele.click()
        items = driver.find_elements(By.CSS_SELECTOR, '#menu > ytd-menu-renderer')
    
        for i, item in tqdm(enumerate(items), total= len(items)):
            try:
                item.click()
            except:
                continue
            try:
                driver.find_element(By.CSS_SELECTOR, '#items > ytd-menu-service-item-renderer:nth-child(2)').click()
            except:
                try:
                    driver.find_element(By.CSS_SELECTOR, '#items > ytd-menu-service-item-renderer:nth-child(3)').click()
                except:
                    continue
            # driver.find_element(By.CSS_SELECTOR, '#copy-button > yt-button-shape > button > yt-touch-feedback-shape > div').click()

            share_url = driver.find_element(By.ID, 'share-url').get_attribute("value")

                
            user_href = driver.find_element(By.XPATH, '//*[@id="text"]/a').get_attribute("href")
            source = {'user_href':user_href, 'share_url':share_url}
            writeSource(path, source)
            try:
                driver.find_element(By.CSS_SELECTOR, '#close-button').click()
            except:
                pass

 
if __name__ == "__main__":
    out_path = 'source.jsonl' # save file.jsonl in path
    # if os.path.isfile(out_path):
    #     os.remove(out_path)
    ytb_url = 'https://www.youtube.com/'
    driver = webdriver.Edge('msedgedriver.exe')
    collectLink(driver, out_path)


