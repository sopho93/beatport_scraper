import unittest
from scraper.scraper import Scraper
from selenium.webdriver.common.by import By
import time



class TestScraper(unittest.TestCase):
    def setUp(self) -> None:
        self.bot = Scraper()
    
    def test_accept_cookies(self): # accept cookies is in the init method so no need to call it again
        #self.bot.accept_cookies()
        time.sleep(2)
        self.bot.driver.find_element(By.XPATH, '//div[@class="header-container"]')
    
    def test_scrape_data(self):
        self.bot.scrape_data()
        actual_value = self.bot.driver.current_url
        expected_value = 'https://www.beatport.com/track/its-a-killa/16252887'  # last page to scrape track data from
        self.assertEqual(expected_value, actual_value)
    
    def test_send_keys(self):
        self.bot.send_keys('Peggy Gou')
        time.sleep(1)
        actual_value = self.bot.driver.current_url
        expected_value = 'https://www.beatport.com/search?q=Peggy+Gou'
        self.assertEqual(expected_value, actual_value)

    def test_click_top_100(self):
        self.bot.click_top_100()
        time.sleep(1)
        actual_value = self.bot.driver.current_url
        expected_value = 'https://www.beatport.com/top-100'
        self.assertEqual(expected_value, actual_value)


    def tearDown(self) -> None:
        pass


unittest.main(argv=[''], exit=False)

if __name__ =='__main__':
    unittest.main()