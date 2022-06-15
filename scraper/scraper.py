import boto3
import config
import json
from multiprocessing.dummy.connection import Client
import os
import pandas as pd
from requests.api import options
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException)
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
from sqlalchemy.future.engine import Engine
import tempfile
import time
import uuid
import urllib.request
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager
import yaml

class Scraper:
    '''
    This class is a scraper that contains some basic methods for browsing various websites

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
            options = self.add_options_arguments(ChromeOptions())
            self.driver = (webdriver.Chrome(service=ChromeService(ChromeDriverManager().
                        install()), options=options))
        else:
            options = self.add_options_arguments(FirefoxOptions())
            self.driver = (webdriver.Firefox(service=FirefoxService(GeckoDriverManager().
                        install()), options=options))
        self.driver.get(url)
        self.driver.maximize_window()

    def add_options_arguments(self, options: options) -> options:
        '''
        This method adds options to the options object

        Parameters
        ----------
        options: options object
            Chrome or Firefox options object
        
        Returns
        ----------
        options: options object
            Returns the value of attribute options with added arguments
        '''
        options = options
        options.add_argument('--headless')  # headless mode
        options.add_argument('--no-sandbox') # needed for docker
        options.add_argument('--disable-dev-shm-usage')  # needed for docker
        # options.add_argument("--window-size=1920,1080")
        # options.add_argument("--remote-debugging-port=9222")
        return options

    def connect_engine(self) -> Engine:
        '''
        This method creates an engine connection using the creds.yaml file in working directory

        Returns
        ----------
        engine: Engine
            An instance of Engine representing the connection to the database
        '''
        try:
            with open('scraper/creds.yaml', 'r') as f: # for Linux in Docker container
                creds = yaml.safe_load(f)
        except FileNotFoundError:
            with open('creds.yaml', 'r') as f:  # for use in Windows
                creds = yaml.safe_load(f)
        
        DATABASE_TYPE = creds['DATABASE_TYPE']
        DBAPI = creds['DBAPI']
        HOST = creds['HOST']
        USER = creds['USER']
        PASSWORD = creds['PASSWORD']
        DATABASE = creds['DATABASE']
        PORT = creds['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def connect_s3_client(self) -> Client:
        '''
        This method asks for input from the user in order to 
        establish an s3 bucket connection

        Returns
        ----------
        client: Client
            An instance of a boto3 client representing the connection to an s3 bucket
        '''
        self.key_id = input('Enter your AWS key id: ')
        self.secret_key = input('Enter your AWS secret key: ')
        self.bucket_name = input('Enter your bucket name: ')
        self.region = input('Enter your region: ')
        client = boto3.client('s3',
                aws_access_key_id = self.key_id,
                aws_secret_access_key = self.secret_key,
                region_name = self.region)
        return client

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
        This method finds and clicks on close button of ads window

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

    def send_keys_to_searchbar(self, xpath: str, text: str) -> None:
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

    def save_data_to_rds(self, current_track_data: dict) -> None:
        '''
        This method saves data to RDS as SQL

        Parameters
        ----------
        current_track_data: dict
            The data to save for the specific track that is being scraped
        '''
        df = pd.DataFrame(current_track_data, index=[0])
        df.to_sql('track_data', con=self.engine, if_exists='append', index=False)

    def create_track_folder(self, folder_name: str = False) -> str:
        '''
        This method creates folders and returns the path to the folder it creates

        Parameters
        ----------
        folder_name: str
            The name we want to give to our track folder
        
        Returns
        -------
        target: str
            The path to the folder that was created
        '''
        cwd = os.getcwd()
        if folder_name:
            target = os.path.join(cwd, 'raw_data', folder_name)
        else:
            target = os.path.join(cwd, 'raw_data')

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
                self.bucket_name, f'{track_title}.jpg')
  
    def quit(self) -> None:
        '''
        This method closes the browser window
        '''
        self.driver.close()
        print('Broser Window Closed')

class BeatportScraper(Scraper):
    '''
    Scraper that works only for browsing Beatport website
    It will extract information about Beatport's top 100 tracks

    Parameters
    ----------
    url: str
        The url to look information about songs from
    chrome: bool
        If the default value=True is used, Chrome browser is used
        If chome is set to False, Firefox is used
    '''
    def __init__(self, chrome: bool = True, url: str = config.URL):
        super().__init__(url, chrome)
        self.accept_cookies(config.ACCEPT_COOKIES)
        self.close_ads(config.CLOSE_ADS)
        self.trackdict = {'Track_Link': [] }
        self.mapping_dict = {0:'Length', 1:'Released', 2:'BPM', 3:'Key', 4:'Genre', 5:'Label'}

    def click_top_100(self, xpath: str) -> None:
        '''
        This method clicks on the TOP 100 button on Beatport website

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
    
    def send_keys_beatport_searchbar(self, text):
        super().send_keys_to_searchbar(config.SEARCH_BAR, text)

    def find_container_and_get_track_links(self, xpath: str) -> None:
        '''
        This method finds container and extracts track links

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
                find_element(By.TAG_NAME, 'a').get_attribute('href'))

    def initialise_saving_method(self, store_locally: bool) -> None:
        '''
        This method initialises the storing method (either local or on the cloud)
        It sets the saving method as an instance attribute so other methods can use it
        For local mode, it creates the initial storing folder if it doesn't exist
        and reads the friendly_ids and track_titles of already scraped tracks
        For online mode it connects to the RDS and S3 and keeps track of the friendly
        ids and track_titles of already scraped tracks

        Parameters
        ----------
        store_locally: bool
            A boolean value denoting if data is to be saved locally or not
        '''
        if store_locally:
            self.store_locally = True
            parent_directory = self.create_track_folder()
            self.find_locally_scraped_tracks(parent_directory)
        else:
            self.store_locally = False
            self.engine = self.connect_engine()
            self.client = self.connect_s3_client()
            self.find_online_scraped_tracks()
        
    def create_current_track_data_dict(self) -> None:
        '''
        This method creates an empty dictionary used for storing track data.
        It creates an empty list for the keys UUID, Friendly_ID, Ranking,
        Track_Title, Track_Link, Artist, Length, Released, BPM, Key, Label,
        Genre and Artwork_Link.
        '''
        self.current_track_data = {'UUID': [], 'Friendly_ID': [], 'Ranking': [],
            'Track_Title': [], 'Track_Link': [], 'Artist': [], 'Length': [], 
            'Released': [], 'BPM': [], 'Key': [], 'Label': [], 'Genre': [], 
            'Artwork_Link': [] }

    def extract_track_info_to_dict(self, xpath: str=config.TRACK_INFO_CONTAINER) -> None:
        '''
        This method updates the current_track_dictionary from values found in the track info container
        It updates dictionary with information about track length, when it was released, its BPM, Key of the
        track, what genre it is classified as and the label that produced it

        Parameters
        ----------
        current_track_data: dict
            The dictionary containing track data to be updated
        xpath: str
            The xpath of the track info container
        '''
        try:
            track_info_container = (self.driver.
                                find_element(By.XPATH, xpath))
            info = (track_info_container.
                find_elements(By.CLASS_NAME, "value"))
            for index, info in enumerate(info):
                self.current_track_data[self.mapping_dict[index]] = info.text
        except NoSuchElementException:
            for value in self.mapping_dict.values():
                self.current_track_data[value] = 'N/A'

    def find_track_artist(self, xpath:str = config.ARTIST_XPATH) -> None:
        '''
        This method finds and updates the artist of the track

        Parameters
        ----------
        xpath: str
            The xpath to the artist section in the track website
        '''
        try:
            artist_section = self.driver.find_element(By.XPATH, xpath)
            artist = artist_section.find_element(By.CLASS_NAME, 'value').text
        except NoSuchElementException:
            artist = 'No artist found'
        self.current_track_data['Artist'] = artist
    
    def find_track_title(self, primary_xpath:str = config.PRIMARY_TITLE_XPATH,
        secondary_xpath:str = config.SECONDARY_TITLE_XPATH) -> None:
        '''
        This method finds and updates the title of the track

        Parameters
        ----------
        primary_xpath: str
            The xpath to the primary title of the track
        secondary_xpath: str
            The xpath to the secondary title of the track
        '''
        try:
            full_title = self.driver.find_element(By.XPATH, primary_xpath)
            primary = (full_title.
                    find_element(By.TAG_NAME, 'h1').text)
            secondary = (full_title.find_element(By.XPATH, secondary_xpath).
                    text)
            track_title = primary + ' ' + secondary
        except NoSuchElementException:
            track_title = 'No track title found'
        self.current_track_data['Track_Title'] = track_title

    def find_artwork_link(self, xpath:str = config.ARTWORK_XPATH) -> None:
        '''
        This method finds and returns the artwork link of the track

        Parameters
        ----------
        xpath: str
            The xpath to the artwork link in the track website
        '''
        self.current_track_data['Artwork_Link'] = (self.driver.find_element
            (By.XPATH, xpath).get_attribute('src'))

    def update_track_dict(self, link: str, friendly_id: str) -> None:
        '''
        This method updates the rank, link, UUID and friendly_id of the current track

        Parameters
        ----------
        link: str
            The link to the track website on Beatport
        friendly_id: str
            The friendly_id associated with this track
        '''
        self.current_track_data['UUID'] = self.new_id
        self.current_track_data['Ranking'] = self.rank
        self.current_track_data['Track_Link'] = link
        self.current_track_data['Friendly_ID'] = friendly_id

    def find_locally_scraped_tracks(self, parent_directory: str) -> None:
        '''
        This method creates two lists, the friendly id and the track title of tracks
        already scraped in local storage

        Parameters
        ----------
        parent_directory: str
            The path to the raw_data folder created
        '''
        self.friendly_id_scraped_local = []
        self.track_titles_scraped_local = []
        for folder in os.listdir(parent_directory):
            data_path = os.path.join(parent_directory, folder, 'data.json')
            f = open(data_path)
            data = json.load(f)
            self.friendly_id_scraped_local.append(data['Friendly_ID']) # type str
            self.track_titles_scraped_local.append(data['Track_Title']) # type str
    
    def find_online_scraped_tracks(self) -> None:
        '''
        This method creates two lists, the friendly id and the track title of tracks
        already scraped on the cloud
        '''
        try:
            df = pd.read_sql('track_data', self.engine)
            self.friendly_id_scraped = list(df['Friendly_ID'])
            self.track_titles_scraped = list(df['Track_Title'])
        except:
            print('First time storing in this RDS')
            self.friendly_id_scraped = []
            self.track_titles_scraped = []


    def check_if_already_scraped(self, friendly_id: str) -> bool:
        '''
        This method, depending on where the data set to be stored, checks if the
        friendly_id provided exists in the list of already scraped tracks
        If it is found, it prints out a message that says the title of the song that 
        was scraped as well as where it was found. Additionally it outputs a variable
        with the value True denoting that it was indeed scraped before
        Otherwise it doesn't print anything and returns False  

        Parameters
        ----------
        friendly_id: str
            The friendly_id to check if song has already been scraped

        Returns
        -------
        scraped: bool
            A boolean value which is True if the track was already scraped and False if not
        '''
        if self.store_locally:
            friendly_ids = self.friendly_id_scraped_local
            track_titles = self.track_titles_scraped_local
            location = 'local storage'
        else:
            friendly_ids = self.friendly_id_scraped
            track_titles = self.track_titles_scraped
            location = 'RDS'
        if friendly_id in friendly_ids:
            name = track_titles[friendly_ids.index(friendly_id)]
            print(f'{name} already scraped in {location}')
            scraped = True
        else:
            scraped = False
        return scraped

    def save_everything_accordingly(self) -> None:
        '''
        This method checks if saving mode is local or on the cloud in order to save the
        data to the corresponding place. It uses the previously defined methods of
        create_track_folder, save_data, save_image_local
        upload_images_to_s3 and save_data_to_rds
        '''
        if self.store_locally:
            track_folder = self.create_track_folder(self.current_track_data['Track_Title']) # create sub folder
            self.save_data(track_folder, self.current_track_data)
            self.save_image_local(track_folder, self.current_track_data['Track_Title'], self.current_track_data['Artwork_Link'])
        else:
            self.upload_images_to_s3(self.current_track_data['Artwork_Link'], self.current_track_data['Track_Title'])
            self.save_data_to_rds(self.current_track_data)

    def scrape_data(self, store_locally=False) -> None:
        '''
        This method scrapes data from the track websites visited
        After it finishes scraping, it closes the web browser

        Parameters
        ----------
        store_locally: bool
            Whether to store scraped data locally or on the cloud
            If nothing is passed as an argument, scraper stores data on the cloud
        '''
        self.click_top_100(config.CLICK_TOP_100)
        self.find_container_and_get_track_links(config.CONTAINER)
        self.initialise_saving_method(store_locally)
        self.rank = 1  # initialize rank
        for link in self.trackdict['Track_Link']:
            friendly_id = link.split('/')[-1]
            scraped = self.check_if_already_scraped(friendly_id)
            if scraped:
                self.rank += 1
                continue
            else:
                self.driver.get(link)
                time.sleep(1)
                self.new_id = str(uuid.uuid4())
                self.create_current_track_data_dict()
                self.find_track_artist()
                self.find_track_title()
                self.extract_track_info_to_dict()
                self.find_artwork_link()
                self.update_track_dict(link, friendly_id)
                self.save_everything_accordingly() 
                print('Scraped ', self.current_track_data['Track_Title'],'!')
                self.rank += 1  # increment rank for next track
        self.quit()

print('====== Beatport Scraper Loaded ======')

if __name__ == "__main__":
    bot = BeatportScraper()
    bot.scrape_data()