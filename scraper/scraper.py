from selenium.webdriver.common.keys import Keys
import urllib.request
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException)
from selenium.webdriver import ActionChains
import time
import uuid
import pandas as pd
import yaml
import boto3
from sqlalchemy import create_engine
import tempfile
import os
import json
from selenium import webdriver
# FOR FIREFOX
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
# FOR CHROME
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions

class Scraper:
    '''
    This class is a scraper that works only for browsing the Beatport website

    Parameters
    ----------
    url: str
        The link that we want to visit
    chrome: bool
        If the default value=True is used, Chrome browser is used
        If chome is set to False, Firefox is used

    Attribute
    ---------
    driver:
        This is the webdriver object
    '''
    def __init__(self, url: str, chrome: bool=True):
        if chrome:
            options = ChromeOptions()
            options.add_argument('--headless')  # remove space to have an effect
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            # options.add_argument("--window-size=1920,1080")
            # options.add_argument("--remote-debugging-port=9222")
            self.driver = (webdriver.Chrome(service=ChromeService(ChromeDriverManager().
                        install()), options=options))
        else:
            options = FirefoxOptions()
            options.add_argument('--headless')  # remove space to have an effect
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            # options.add_argument("--window-size=1920,1080")
            # options.add_argument("--remote-debugging-port=9222")
            self.driver = (webdriver.Firefox(service=FirefoxService(GeckoDriverManager().
                        install()), options=options))

        self.driver.get(url)
        self.driver.maximize_window()
        
        try:
            with open('scraper/creds.yaml', 'r') as f: # for linux use in docker
                creds = yaml.safe_load(f)
        except FileNotFoundError:
            with open('creds.yaml', 'r') as f:  # for use in windows
                creds = yaml.safe_load(f)
        
        DATABASE_TYPE = creds['DATABASE_TYPE']
        DBAPI = creds['DBAPI']
        HOST = creds['HOST']
        USER = creds['USER']
        PASSWORD = creds['PASSWORD']
        DATABASE = creds['DATABASE']
        PORT = creds['PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        
        self.key_id = input('Enter your AWS key id: ')
        self.secret_key = input('Enter your AWS secret key: ')
        self.bucket_name = input('Enter your bucket name: ')
        self.region = input('Enter your region: ')
        self.client = boto3.client('s3',
                                   aws_access_key_id=self.key_id,
                                   aws_secret_access_key=self.secret_key,
                                   region_name=self.region)

    def accept_cookies(self, xpath: str) -> None:
        '''
        This method looks for and clicks on the accept cookies button

        Parameters
        ----------
        xpath: str
            The xpath of the accept cookies button
        '''
        try:
            time.sleep(1)
            (WebDriverWait(self.driver, 10).
                until(EC.presence_of_all_elements_located((By.XPATH, xpath))))
            self.driver.find_element(By.XPATH, xpath).click()
        except TimeoutException:
            print('No cookies found')

    def close_ads(self, xpath: str) -> None:
        '''
        This methods finds and clicks on close button of ads window

        Parameters
        ----------
        xpath: str
            The xpath of the close ads button
        '''
        try:
            time.sleep(1)
            self.driver.find_element(By.XPATH, xpath).click()
        except NoSuchElementException:
            print('No ads window found')

    def scroll_to(self, length: int) -> None:
        '''
        This method scrolls to a specific vertical length in the  webpage

        Parameters
        ----------
        length: int
            The vertical length you want to scroll to
        '''
        self.driver.execute_script(f'window.scrollTo(0,{length})')

    def click_top_100(self, xpath: str) -> None:
        '''
        This method clicks on the TOP 100 button

        Parameters
        ----------
        xpath: str
            The xpath of the Beatport Top 100 button
        '''
        #need to scroll down so that element is visible
        element = self.driver.find_element(By.XPATH, xpath) 
        element_location = element.location
        self.scroll_to(int(element_location['y']-300))
        
        try:
            actions = ActionChains(self.driver)
            actions.move_to_element(element).perform()
            element.click()
        except ElementClickInterceptedException:
            self.scroll_to(int(element_location['y']))
            self.driver.find_element(By.XPATH, xpath).click()

    def find_search_bar(self, xpath: str):
        '''
        This method finds and returns the search bar in the webpage

        Parameters
        ----------
        xpath: str
            The xpath of the search bar

        Returns
        -------
        search_bar: webdriver.element
            If found returns the search bar as a webdriver.element

        '''
        time.sleep(1)
        try:
            search_bar = (WebDriverWait(self.driver, 5).until(EC
                          .presence_of_element_located((By.XPATH, xpath))))
            search_bar.find_element(By.XPATH, xpath)
            return search_bar
        except NoSuchElementException:
            print('No search bar found')

    def send_keys(self, xpath: str, text: str) -> None:
        '''
        Method that writes something in search bar

        Parameters
        ----------
        text: str
            The text we want to pass to the search bar
        xpath: str
            The xpath of the search bar
        '''
        search_bar = self.find_search_bar(xpath)
        if search_bar:
            search_bar.click()
            search_bar.send_keys(text)
            search_bar.send_keys(Keys.ENTER)
        else:
            raise Exception('No search bar found, therefore no keys were send')

    def find_container_and_get_track_links(self, xpath: str) -> None:
        '''
        Function that finds container and extracts track links

        Parameters
        ----------
        xpath: str
            The xpath of the container
        '''
        container = (WebDriverWait(self.driver, 10).until(EC.
                     presence_of_element_located((By.XPATH, xpath))))
        container.find_element(By.XPATH, xpath)
        list_tracks = container.find_elements(By.XPATH, './li')
        for track in list_tracks:
            track_link_container = track.find_element(By.TAG_NAME, 'p')
            self.trackdict['Track_Link'].append(track_link_container.
                                                find_element(By.TAG_NAME, 'a').
                                                get_attribute('href'))

    def save_data_to_rds(self, current_track_data: dict) -> None:
        '''
        This method saves data to rds as sql

        Parameters
        ----------
        current_track_data: dict
            The data to save for the specific track that is being scraped
        '''
        df = pd.DataFrame(current_track_data, index=[0])
        df.to_sql('track_data', con=self.engine, if_exists='append', index=False)


    def create_raw_data_folder(self) -> None:
        '''
        This method creates a folder for storing the scraped data locally
        '''
        cwd = os.getcwd()
        target = os.path.join(cwd, 'raw_data')

        # Check whether the specified folder exists or not
        if not os.path.exists(target):
            os.mkdir('raw_data')

    def create_track_folder(self, folder_name: str) -> None:
        '''
        This method creates folders inside the raw data folder for each track

        Parameters
        ----------
        folder_name: str
            The name we want to give to our track folder
        '''
        cwd = os.getcwd()
        target = os.path.join(cwd, 'raw_data', folder_name)

        # Check whether the specified folder exists or not
        if not os.path.exists(target):
            os.mkdir(target)
        return(target)

    def save_data(self, folder: str, data: dict) -> None:
        '''
        This mathod saves data locally in JSON format

        Parameters
        ----------
        folder: str
            The folder where the data will be saved
        data: dict
            A dictionary containing the data for each track

        '''
        with open(f'{folder}/data.json', 'w') as f:
            json.dump(data, f)

    def save_image_local(self, folder: str, title: str, link: str) -> None:
        '''
        This method saves image locally for given track

        Parameters
        ----------
        folder: str
            The local folder in which the image will be saved
        title: str
            The title we want to give to the saved image
        link: str
            The link to the image

        '''
        urllib.request.urlretrieve(link, f'{folder}/{title}.jpg')

    def quit(self):
        '''
        This method closes the browser window
        '''
        self.driver.close()
        print('Broser Window Closed')

    def upload_images_to_s3(self, link: str, track_title: str) -> None:
        '''
        This method uploads images to the s3 bucket

        Parameters
        ----------
        link: str
            The link to the image we want to save
        track_title: str
            The name of the image will be the same as the track_title
        '''
        with tempfile.TemporaryDirectory() as tmpdirname:
            urllib.request.urlretrieve(link, tmpdirname +
                                        f'/{track_title}.jpg')
            self.client.upload_file(tmpdirname + f'/{track_title}.jpg',
                                    'aicore-beatport', f'{track_title}.jpg')

class BeatportScraper(Scraper):
    '''
    Scraper that works only for browsing Beatport website
    It will extract information about Beatport's top 100 songs

    Parameters
    ----------
    url: str
        The url to look information about songs from
    chrome: bool
        If the default value=True is used, Chrome browser is used
        If chome is set to False, Firefox is used
    '''
    def __init__(self, chrome: bool = True, url: str = 'https://www.beatport.com/'):
        super().__init__(url, chrome)
        self.accept_cookies('//button[@id="CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"]')
        self.close_ads('//div[@id="nosto-close"]')
        self.trackdict = {
                    'UUID': [],
                    'Friendly_ID': [],
                    'Ranking': [],
                    'Track_Title': [],
                    'Track_Link': [],
                    'Artist': [],
                    'Length': [],
                    'Released': [],
                    'BPM': [],
                    'Key': [],
                    'Label': [],
                    'Genre': [],
                    'Artwork_Link': []
        }
    def find_beatport_search_bar(self, xpath: str=
                                '//input[@class="text-input__input text-input__input--no-margin"]'):
        '''
        This method finds and returns the search bar in Beatport webpage

        Parameters
        ----------
        xpath: str
            The xpath of the search bar

        Returns
        -------
        beatport_search_bar: webdriver.element
            If found returns the search bar as a webdriver.element

        '''
        time.sleep(1)
        try:
            search_bar = (WebDriverWait(self.driver, 5).until(EC
                          .presence_of_element_located((By.XPATH, xpath))))
            search_bar.find_element(By.XPATH, xpath)
            return search_bar
        except NoSuchElementException:
            print('No search bar found')   

    def send_keys_beatport(self, text: str) -> None:
        '''
        Method that writes something in Beatport search bar

        Parameters
        ----------
        text: str
            The text we want to pass to the search bar
        '''
        search_bar = self.find_beatport_search_bar()
        if search_bar:
            search_bar.click()
            search_bar.send_keys(text)
            search_bar.send_keys(Keys.ENTER)
        else:
            raise Exception('No search bar found, therefore no keys were send')

    def scrape_data(self, store_locally=False):
        '''
        This method scrapes data from the track websites visited

        Parameters
        ----------
        store_locally: bool
            Whether to store scraped data locally or on the cloud
            If nothing is passed as an argument, scraper stores data on the cloud
        '''
        self.click_top_100('//a[@class="view-top-hundred-tracks"]')
        self.find_container_and_get_track_links('//ul[@class="bucket-items  ec-bucket"]')
        if store_locally:
            self.create_raw_data_folder()
        else:
            self.engine.connect()
            df = pd.read_sql('track_data', self.engine)
            self.friendly_id_scraped = list(df['Friendly_ID'])
            self.track_titles_scraped = list(df['Track_Title'])
        rank = 1  # initialize rank

        for link in self.trackdict['Track_Link']:#[0:3]:  # change for all top100 add [0:3] for testing
            friendly_id = link.split('/')[-1]
            if friendly_id in self.trackdict['Friendly_ID']:
                print('Track already found in local storage')
                rank += 1  # increment rank for next track
                continue
            elif friendly_id in self.friendly_id_scraped:
                name = self.track_titles_scraped[self.friendly_id_scraped.index(friendly_id)]
                print(f'{name} already scraped in RDS')
                rank += 1
                continue
            else:
                self.driver.get(link)
                new_id = str(uuid.uuid4())
                self.trackdict['Friendly_ID'].append(friendly_id)
                self.trackdict['UUID'].append(new_id)
                self.trackdict['Ranking'].append(rank)
                time.sleep(1)

                current_track_data = {'UUID': [],
                                      'Friendly_ID': [],
                                      'Ranking': [],
                                      'Track_Title': [],
                                      'Track_Link': [],
                                      'Artist': [],
                                      'Length': [],
                                      'Released': [],
                                      'BPM': [],
                                      'Key': [],
                                      'Label': [],
                                      'Genre': [],
                                      'Artwork_Link': []
                                      }

                # get and append artist actions
                try:
                    xpath = '//div[@class="interior-track-artists"]'
                    artist_section = self.driver.find_element(By.XPATH, xpath)
                    artist = artist_section.find_element(By.CLASS_NAME, 'value').text
                    self.trackdict['Artist'].append(artist)
                except NoSuchElementException:
                    self.trackdict['Artist'].append('No artist found')

                try:
                    xpath = '//div[@class="interior-title"]'
                    full_title = self.driver.find_element(By.XPATH, xpath)
                    primary = (full_title.
                               find_element(By.TAG_NAME, 'h1').text)
                    xpath = '//h1[@class="remixed"]'
                    secondary = (full_title.find_element(By.XPATH, xpath).
                                 text)
                    track_title = primary + ' ' + secondary
                    self.trackdict['Track_Title'].append(track_title)
                except NoSuchElementException:
                    self.trackdict['Track_Title'].append('No track title found')

                # track info container
                xpath = '//ul[@class = "interior-track-content-list"]'
                try:
                    track_info_container = (self.driver.
                                            find_element(By.XPATH, xpath))
                    info = (track_info_container.
                            find_elements(By.CLASS_NAME, "value"))
                    for index, info in enumerate(info):
                        if index == 0:
                            self.trackdict['Length'].append(info.text)
                            current_track_data['Length'] = info.text
                        elif index == 1:
                            self.trackdict['Released'].append(info.text)
                            current_track_data['Released'] = info.text
                        elif index == 2:
                            self.trackdict['BPM'].append(info.text)
                            current_track_data['BPM'] = info.text
                        elif index == 3:
                            self.trackdict['Key'].append(info.text)
                            current_track_data['Key'] = info.text
                        elif index == 4:
                            self.trackdict['Genre'].append(info.text)
                            current_track_data['Genre'] = info.text
                        else:
                            self.trackdict['Label'].append(info.text)
                            current_track_data['Label'] = info.text
                except NoSuchElementException:
                    self.trackdict['Length'].append('N/A')
                    current_track_data['Length'] = 'N/A'
                    self.trackdict['Released'].append('N/A')
                    current_track_data['Released'] = 'N/A'
                    self.trackdict['BPM'].append('N/A')
                    current_track_data['BPM'] = 'N/A'
                    self.trackdict['Key'].append('N/A')
                    current_track_data['Key'] = 'N/A'
                    self.trackdict['Genre'].append('N/A')
                    current_track_data['Genre'] = 'N/A'
                    self.trackdict['Label'].append('N/A')
                    current_track_data['Label'] = 'N/A'

                # get the artwork link
                xpath = '//img[@class= "interior-track-release-artwork"]'
                artwork_link = (self.driver.find_element
                                (By.XPATH, xpath).get_attribute('src'))
                self.trackdict['Artwork_Link'].append(artwork_link)

                current_track_data['UUID'] = new_id
                current_track_data['Friendly_ID'] = friendly_id
                current_track_data['Ranking'] = rank
                current_track_data['Track_Title'] = track_title
                current_track_data['Track_Link'] = link
                current_track_data['Artist'] = artist
                current_track_data['Artwork_Link'] = artwork_link

                if store_locally:
                    # create sub folder
                    track_folder = self.create_track_folder(track_title)
                    self.save_data(track_folder, current_track_data)
                    self.save_image_local(track_folder, track_title, artwork_link)
                else:
                    self.upload_images_to_s3(artwork_link, track_title)
                    self.save_data_to_rds(current_track_data)

                print(f'Scraped {track_title}!')
                rank += 1  # increment rank for next track
        self.quit()

print('====== Beatport Scraper Loaded ======')

if __name__ == "__main__":
    bot = BeatportScraper()
    bot.scrape_data()