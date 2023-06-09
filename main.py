import datetime
import logging
import pickle
import random
import time

import googlemaps
import janitor
import pandas as pd
import pygsheets
from linkedin_scraper import actions
from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm.auto import tqdm

import credentials


class runner:
    def __init__(self):
        """This class will use selenium to load the chrome driver and login to LinkedIn."""
        self.email = credentials.email
        self.password = credentials.password

        self.infos = pd.DataFrame(
            columns=[
                "uid",
                "name",
                "image",
                "location",
                "employer",
                "alma_mater",
                "url",
                "optout",
                "last_update",
            ]
        )

        self.group_list = []

        option = webdriver.ChromeOptions()
        option.add_argument("--disable-blink-features=AutomationControlled")
        self.browser = webdriver.Chrome(options=option)

        time.sleep(1)

        self.browser.get(
            "https://www.linkedin.com"
        )  # Open any page just to load the cookies

        time.sleep(1)

    def query_gs(self, workbook, sheet) -> pd.DataFrame:
        """This function will query a Google Sheet and return a pandas dataframe.

        Args:
            workbook (str): the python object with the spreadsheet opened using pygsheets
            sheet (int): the index of the sheet to be queried

        Returns:
            pandas.DataFrame: all the columns and values of the sheet"""

        return pd.DataFrame(workbook[sheet].get_all_records())

    def quantum_value(self, value: int, list: list):
        """This function checks if an index exists in list. If it does, it will return the 'text' attribute of the element.
        If it doesn't, it will return None.

        Args:
            value (int): the index to be checked
            list (list): the list to be checked

        Returns:
            str: the text attribute of the element if it exists, None otherwise"""
        if len(list) >= value + 1:
            return list[value].text
        else:
            return ""

    def get_geo(self, address: str) -> tuple:
        """This function will use the Google Maps API to get geoinformation about a location. It will get the centroid
        of the area to always return the administrative_area_level_2 or administrative_area_level_1 of that area in the final results.

        Args:
            address (str): the address to be geocoded. In this case, it is advised to always use an area (city, state, country, etc.)

        Returns:
            tuple: the latlng of the centroid of the area given, the latlng of the centroid of the area, the name of the area, the type of the
            area, the country of the area
        """
        gmaps = googlemaps.Client(key=credentials.gmaps_key)
        geocode_result = gmaps.geocode(address)
        original_latlng = geocode_result[0]["geometry"]["location"]
        location_type = geocode_result[0]["types"][0]

        reverse_geocode_result = gmaps.reverse_geocode(
            original_latlng, result_type=["administrative_area_level_2"]
        )
        if reverse_geocode_result == []:
            reverse_geocode_result = gmaps.reverse_geocode(
                original_latlng, result_type=["administrative_area_level_1"]
            )

        area_latlng = reverse_geocode_result[0]["geometry"]["location"]
        area_name = reverse_geocode_result[0]["formatted_address"]
        country = reverse_geocode_result[0]["address_components"][-1]["long_name"]

        return (original_latlng, area_latlng, area_name, location_type, country)

    def login(self, method):
        """Basic function to execute a login in LinkedIn using the linkedin_scraper package"""

        if method == "linkedin_scraper":
            actions.login(self.browser, self.email, self.password)
        elif method == "cookies":
            with open("cookies.pkl", "rb") as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    self.browser.add_cookie(cookie)

        time.sleep(2)

    def random_sleep(self, min, max):
        """This function will sleep for a random amount of time between min and max seconds. Ideal for scrapping.

        Args:
            min (int): the minimum amount of seconds to sleep
            max (int): the maximum amount of seconds to sleep"""

        time.sleep(random.uniform(min, max))

    def retrieve_info(self) -> tuple:
        """This function will get information from both the first and second sheets of the file indicated on credentials.py
        as well as for the file contained the opt-out form data and return as Pandas DataFrames inside a tuple.
        """

        gs = pygsheets.authorize(service_account_file="gsheet_credential.json")
        wb_main = gs.open_by_key(credentials.gsheets_main_key)
        main = self.query_gs(wb_main, 0)
        optin = self.query_gs(wb_main, 1)

        wb_form = gs.open_by_key(credentials.gsheets_form_key)
        optout = self.query_gs(wb_form, 0).clean_names()

        logging.info("Retrieved data from Google Sheets")
        logging.info(
            f"Rows in Main: {len(main)}, optin: {len(optin)}, optout: {len(optout)}"
        )

        return (main, optin, optout)

    def get_members_list(self) -> list:
        """This function will get the list of members of the groups indicated on credentials.py and return it as a list.

        Returns:
            list: the url list of of all members of the groups indicated on credentials.py
        """
        browser = self.browser
        browser.get(f"https://www.linkedin.com/groups/{credentials.group_id}/members/")

        time.sleep(3)
        last_height = browser.execute_script("return document.body.scrollHeight")

        cont = 0

        while True:
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            self.random_sleep(3, 7)

            new_height = browser.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                browser.set_window_size(
                    random.randint(500, 1000), random.randint(500, 1000)
                )
                cont += 1

            if cont > 1:
                break

            last_height = new_height

        browser.set_window_size(1920, 1080)
        members = browser.find_elements(
            By.CLASS_NAME,
            "ember-view.ui-conditional-link-wrapper.ui-entity-action-row__link",
        )

        members = [member.get_attribute("href") for member in members]
        logging.info(f"Number of members: {len(members)}")

        return members

    def scraper(self, fellow: str) -> dict:
        """This function will take an URL containing a LinkedIn profile and scrap it to obtain the person's name,
        their picture, current location and employer and the highlighted education institution

        Args:
            fellow (str): full URL of a Linkedin profile

        Returns:
            dict: dictionary with uid (same as url in this case), name on the profile, image url, location,
            employer, and highlithed education institution and the url once more
        """
        browser = self.browser

        browser.get(fellow)

        self.random_sleep(30, 60)

        location = browser.find_elements(
            By.CLASS_NAME, "text-body-small.inline.t-black--light.break-words"
        )
        location = self.quantum_value(0, location)

        image = browser.find_elements(
            By.CLASS_NAME,
            "pv-top-card-profile-picture__image.pv-top-card-profile-picture__image--show.ember-view",
        )
        if image == []:
            image = ""
        else:
            image = image[0].get_attribute("src")

        name = browser.find_elements(
            By.CLASS_NAME, "text-heading-xlarge.inline.t-24.v-align-middle.break-words"
        )
        name = name[0].text

        associations = browser.find_elements(
            By.CLASS_NAME,
            "inline-show-more-text.inline-show-more-text--is-collapsed.inline-show-more-text--is-collapsed-with-line-clamp.inline",
        )

        employer = self.quantum_value(0, associations)
        alma_mater = self.quantum_value(1, associations)

        logging.info(f"Scrapped data for {name}")

        return {
            "uid": fellow,
            "name": name,
            "image": image,
            "location": location,
            "employer": employer,
            "alma_mater": alma_mater,
            "url": fellow,
        }

    def add_geoinfo(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds the geoinformation for each row on the dataframe.

        Args:
            df (pd.DataFrame): dataframe with at least the columns "location" and "country"

        Returns:
            pd.DataFrame: the same dataframe, now with the columns "original_latlng", "area_latlng",
            "area_name", "location_type", and "country" added
        """

        df["original_latlng"] = df["location"].apply(lambda x: self.get_geo(x)[0])
        df["area_latlng"] = df["location"].apply(lambda x: self.get_geo(x)[1])
        df["area_name"] = df["location"].apply(lambda x: self.get_geo(x)[2])
        df["location_type"] = df["location"].apply(lambda x: self.get_geo(x)[3])
        df["country"] = df["location"].apply(lambda x: self.get_geo(x)[4])

        df["area_name"] = df.apply(
            lambda x: x["location"]
            if x["location_type"] == "country"
            else x["area_name"],
            axis=1,
        )

        return df

    def update(self, all: bool = False):
        """Simply run this function to automatically update the google sheet containing the data of the members
        with new members or people added to the opt-in sheet.

        Args:
            all (bool, optional): If True, it will scrap again information for everyone. If not specified, then it
            will just scrap and append data for people not previously included. Defaults to False.
        """

        self.login("cookies")
        main, optin, optout = self.retrieve_info()
        old_uid = main["uid"].tolist()

        members_list = self.get_members_list()
        new_uid = set(members_list + optin["uid"].tolist()) - set(old_uid)

        if all is True:
            new_uid.update(old_uid)

        for uid in tqdm(new_uid, desc="Loading fellows"):
            if uid.startswith("https://www.linkedin.com/in/"):
                infos_lin = self.scraper(uid)
                infos_lin["last_update"] = datetime.datetime.now().strftime("%Y-%m-%d")
                self.infos = pd.concat([self.infos, pd.DataFrame([infos_lin])])

            else:
                self.infos = pd.concat([self.infos, optin.query("uid == @uid")])

        self.infos = self.add_geoinfo(self.infos)
        self.infos = pd.concat([self.infos, main])

        self.infos["optout"] = self.infos.apply(
            lambda x: 1 if x["uid"] in optout[optout.columns[1]].tolist() else 0, axis=1
        )

        gs = pygsheets.authorize(service_account_file="gsheet_credential.json")
        wb_main = gs.open_by_key(credentials.gsheets_main_key)
        main = self.query_gs(wb_main, 0)
        wb_main[0].clear()
        wb_main[0].set_dataframe(self.infos, (1, 1))


if __name__ == "__main__":
    runner = runner()
    runner.update()
