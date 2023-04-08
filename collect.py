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

    items = driver.find_elements(By.CSS_SELECTOR, '#menu > ytd-menu-renderer')
    items = items[:10]
    users_href = driver.find_elements(By.XPATH, '//*[@id="text"]/a')
    users_href = users_href[:10]
    for item, href in tqdm(zip(items, users_href), total= len(items)):
        try:
            item.click()
        except:
            continue
        try:
            driver.implicitly_wait(10)
            driver.find_element(By.CSS_SELECTOR, '#items > ytd-menu-service-item-renderer:nth-child(2)').click()
            
        except:
            driver.implicitly_wait(10)
            driver.find_element(By.CSS_SELECTOR, '#items > ytd-menu-service-item-renderer:nth-child(3)').click()
            
        # driver.find_element(By.CSS_SELECTOR, '#copy-button > yt-button-shape > button > yt-touch-feedback-shape > div').click()
        driver.implicitly_wait(10)
        share_url = driver.find_element(By.ID, 'share-url').get_attribute("value")
        driver.implicitly_wait(10)            
        user_href = href.get_attribute("href")
        source = {'user_href':user_href, 'share_url':share_url}
        writeSource(path, source)
        try:
            driver.implicitly_wait(10)
            driver.find_element(By.CSS_SELECTOR, '#close-button').click()

        except:
            pass

 
if __name__ == "__main__":
    out_path = 'source.jsonl' # save file.jsonl in path
    # if os.path.isfile(out_path):
    #     os.remove(out_path)
    ytb_url = 'https://www.youtube.com/feed/trending?bp=4gINGgt5dG1hX2NoYXJ0cw%3D%3D' #top 30 music trending
    driver = webdriver.Edge('msedgedriver.exe')
    collectLink(driver, out_path)


