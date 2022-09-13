import datetime
import logging
import random
import time

import googlemaps
import janitor
import pandas as pd
import pygsheets
from linkedin_scraper import actions
from selenium import webdriver
from tqdm.auto import tqdm

import credentials

class runner():

    def __init__(self):
        self.email = credentials.email
        self.password = credentials.password

        self.infos = pd.DataFrame(columns=['uid', 'name', 'image', 'location', 'employer', 'alma_mater', 'url', 'optout', 'last_update'])

        self.group_list = []

        option = webdriver.ChromeOptions()
        option.add_argument('--disable-blink-features=AutomationControlled')
        self.browser = webdriver.Chrome(options=option)

        time.sleep(3)
        actions.login(self.browser, self.email, self.password)

    def query_gs(self, workbook, sheet) -> pd.DataFrame:
        return pd.DataFrame(workbook[sheet].get_all_records())

    def quantum_value(self, value: int, list: list):
        if len(list) >= value+1:
            return list[value].text
        else:
            return ''

    def get_geo(self, address: str):
        gmaps = googlemaps.Client(key=credentials.gmaps_key)
        geocode_result = gmaps.geocode(address)
        original_latlng = geocode_result[0]['geometry']['location']
        location_type = geocode_result[0]['types'][0]

        reverse_geocode_result = gmaps.reverse_geocode(original_latlng, result_type=['administrative_area_level_2'])
        if reverse_geocode_result == []:
            reverse_geocode_result = gmaps.reverse_geocode(original_latlng, result_type=['administrative_area_level_1'])

        area_latlng = reverse_geocode_result[0]['geometry']['location']
        area_name = reverse_geocode_result[0]['formatted_address']
        country = reverse_geocode_result[0]['address_components'][-1]['long_name']

        return (original_latlng, area_latlng, area_name, location_type, country)

    def login(self):
        actions.login(self.browser, self.email, self.password)
    def random_sleep(self, min, max):
        time.sleep(random.uniform(min, max))

    def retrieve_info(self):
        gs = pygsheets.authorize(service_file='gsheet_credential.json')
        wb_main = gs.open_by_key(credentials.gsheets_main_key)
        main = self.query_gs(wb_main, 0)
        optin = self.query_gs(wb_main, 1)

        wb_form = gs.open_by_key(credentials.gsheets_form_key)
        optout = self.query_gs(wb_form, 0).clean_names()

        logging.info('Retrieved data from Google Sheets')
        logging.info(f'Rows in Main: {len(main)}, optin: {len(optin)}, optout: {len(optout)}')

        return (main, optin, optout)

    def get_members_list(self) -> list:
        browser = self.browser
        browser.get(f'https://www.linkedin.com/groups/{credentials.group_id}/members/')

        time.sleep(3)
        last_height = browser.execute_script("return document.body.scrollHeight")

        cont = 0

        while True:

            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            self.random_sleep(3,7)

            new_height = browser.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                browser.set_window_size(random.randint(500,1000), random.randint(500,1000))
                cont += 1

            if cont > 1:
                break

            last_height = new_height

        browser.set_window_size(1920, 1080)
        members = browser.find_elements_by_class_name("ember-view.ui-conditional-link-wrapper.ui-entity-action-row__link")

        members = [member.get_attribute("href") for member in members]
        logging.info(f'Number of members: {len(members)}')

        return members

    def scraper(self, fellow: str) -> dict:
        browser = self.browser

        browser.get(fellow)

        self.random_sleep(30,60)

        location = browser.find_elements_by_class_name("text-body-small.inline.t-black--light.break-words")
        location = self.quantum_value(0, location)

        image = browser.find_elements_by_class_name("pv-top-card-profile-picture__image.pv-top-card-profile-picture__image--show.ember-view")
        if image == []:
            image = ''
        else:
            image = image[0].get_attribute("src")

        name = browser.find_elements_by_class_name("text-heading-xlarge.inline.t-24.v-align-middle.break-words")
        name = name[0].text

        associations = browser.find_elements_by_class_name("inline-show-more-text.inline-show-more-text--is-collapsed.inline-show-more-text--is-collapsed-with-line-clamp.inline")

        employer = self.quantum_value(0, associations)
        alma_mater = self.quantum_value(1, associations)

        logging.info(f'Scrapped data for {name}')

        return {'uid': fellow, 'name': name, 'image': image, 'location': location, 'employer': employer, 'alma_mater': alma_mater, 'url': fellow}

    def add_geoinfo(self, df: pd.DataFrame) -> pd.DataFrame:
        # For all rows, add latlng on another column based on location
        df['original_latlng'] = df['location'].apply(lambda x: self.get_geo(x)[0])
        df['area_latlng'] = df['location'].apply(lambda x: self.get_geo(x)[1])
        df['area_name'] = df['location'].apply(lambda x: self.get_geo(x)[2])
        df['location_type'] = df['location'].apply(lambda x: self.get_geo(x)[3])
        df['country'] = df['location'].apply(lambda x: self.get_geo(x)[4])

        df['area_name'] = df.apply(lambda x: x['location'] if x['location_type'] == 'country' else x['area_name'], axis=1)

        return df

    def update(self, all: bool = False):
        main, optin, optout = self.retrieve_info()
        old_uid = main['uid'].tolist()

        members_list = self.get_members_list()
        new_uid = set(members_list + optin['uid'].tolist()) - set(old_uid)
        if all == True:
            new_uid.update(old_uid)

        for uid in tqdm(new_uid, desc= "Loading fellows"):
            if uid.startswith('https://www.linkedin.com/in/'):
                infos_lin = self.scraper(uid)
                infos_lin['last_update'] = datetime.datetime.now().strftime('%Y-%m-%d')
                self.infos = pd.concat([self.infos, pd.DataFrame([infos_lin])])

            else:
                self.infos = pd.concat([self.infos, optin.query('uid == @uid')])

        self.infos = self.add_geoinfo(self.infos)
        self.infos = pd.concat([self.infos, main])
        self.infos = pd.concat([self.infos, main])

        self.infos['optout'] = self.infos.apply(lambda x: 1 if x['uid'] in optout[optout.columns[1]].tolist() else 0, axis=1)

        gs = pygsheets.authorize(service_file='gsheet_credential.json')
        wb_main = gs.open_by_key(credentials.gsheets_main_key)
        main = self.query_gs(wb_main, 0)
        wb_main[0].clear()
        wb_main[0].set_dataframe(self.infos, (1,1))

if __name__ == '__main__':
    runner = runner()
    runner.update()

