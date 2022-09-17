# 🗺 dssg-world-tracker

This is the code used to generate and update the DSSG World Tracker spreadsheet.
It can be replicated to scrap any LinkedIn group to obtain information on its members.

_PLEASE NOTE:_ LinkedIn's terms of service do not allow automated scrapping. It is advised to create an alternative account before using this code so that you do not get banned or suspended.

You will need first to create two spreadsheets on Google Sheets. One will contain the main information sheet (that you can later use to produce dashboards etc) and the optin sheet, the other will contain information on opt-out requests.

Rename the credentials.sample file to credentials.py and replace all variables with the appropriate values.
You will also need a *gsheet_credential.json* file on the main repository. Check the [Google Sheets API documentation](https://developers.google.com/sheets/api/quickstart/python) for more information.

This code has been not replicated elsewhere, so many errors could occur. Feel free to open an issue if you find any.
