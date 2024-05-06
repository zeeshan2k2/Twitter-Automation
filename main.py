import os
import shutil
import time
from datetime import timedelta
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions

# opening twitter login page
chrome_options = ChromeOptions()
service = Service(r"/Users/zeeshanwaheed/Desktop/PycharmProjects1/Twitter-Automation/chromedriver")
browser = webdriver.Chrome(service=service, options=chrome_options)
browser.get("https://twitter.com/i/flow/login")
time.sleep(13)

# entering username
buttonEnterDetails = browser.find_element(By.XPATH, '//input[@name="text"]')
# Enter username in strings
buttonEnterDetails.send_keys()
buttonEnterDetails.send_keys(Keys.ENTER)
time.sleep(5)

# entering password
clickNextButton = browser.find_element(By.XPATH, '//*[@name="password"]')
# Enter password in strings
clickNextButton.send_keys()
clickNextButton.send_keys(Keys.ENTER)
time.sleep(15)

# clicking on search button
clickSearchButton = browser.find_element(By.XPATH,
                                         '//*[@id="react-root"]/div/div/div[2]/header/div/div/div/div[1]/div[2]/nav/a[2]')
clickSearchButton.click()
time.sleep(6)

# clicking on search input field and entering text
clickSearchBarInput = browser.find_element(By.XPATH,
                                           '//*[@id="react-root"]/div/div/div[2]/main/div/div/div/div/div/div[1]/div[1]/div/div/div/div/div/div[1]/div[2]/div/div/div/form/div[1]/div/div/div/label/div[2]/div/input')
clickSearchBarInput.send_keys("#connect")
clickSearchBarInput.send_keys(Keys.ENTER)
time.sleep(8)


# ArrForprevoiusTweet = []

def commenting():
    clickTweet = browser.find_element(By.XPATH, '//*[@data-testid="tweetText"]')
    clickTweet.click()
    time.sleep(8)
    # entering comment
    clickRepySection = browser.find_element(By.XPATH,
                                            '//*[@id="react-root"]/div/div/div[2]/main/div/div/div/div/div/section/div/div/div[1]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div/div/div/div/div[1]/div/div/div/div/div/label/div[1]/div/div/div/div/div/div[2]/div/div/div/div')

    clickRepySection.send_keys("Let's Connect")
    time.sleep(8)
    # clicking reply button
    clickRepyButton = browser.find_element(By.XPATH,
                                           '//*[@id="react-root"]/div/div/div[2]/main/div/div/div/div/div/section/div/div/div[1]/div/div/div/div/div[2]/div[2]/div/div/div/div[2]/div[2]/div[2]/div/div/div/div/div/span/span')
    clickRepyButton.click()
    # clickTweetFirst.clear()
    # clickTweetFirst.append(browser.find_element(By.XPATH, '//*[@data-testid="tweetText"]'))
    time.sleep(10)
    # else:
    #     return
    # clickTweetFirst = browser.find_element(By.XPATH, '//*[@data-testid="tweetText"]')
    # browser.find_element(By.XPATH, '//*[@data-testid="tweetText"]')


commenting()
# to click back buttoon
backButtonClick = browser.find_element(By.XPATH,
                                       '//*[@id="react-root"]/div/div/div[2]/main/div/div/div/div/div/div[1]/div[1]/div/div/div/div/div/div[1]/div/div')
backButtonClick.click()

EnterButton = browser.find_element(By.XPATH, '//*[@data-testid="SearchBox_Search_Input"]')
EnterButton.send_keys(Keys.ENTER)
# to scroll down
time.sleep(2)
# browser.execute_script("window.scrollTo(0,2000)")
print("Back button clicking")
time.sleep(10)

# getting browser url
currentUrl = browser.current_url
html = browser.page_source
# print(currentUrl)
# print(html)

# openingTextFile = open("Twitter-SourceCode", "a")
# openingTextFile.write(html)
print()
print()
print()
# print(html.find('class"'))
time.sleep(10000)
