import pandas as pd
import gspread
from datetime import datetime
import yaml

from oauth2client.service_account import ServiceAccountCredentials

# define the scope
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# add credentials to the account
CREDS = ServiceAccountCredentials.from_json_keyfile_name('misc/personalfinance-347102-711f9c425e37.json', SCOPE)
# authorize the client
CLIENT = gspread.authorize(CREDS)

# load column dtypes file to dict 
DTYPES = yaml.safe_load(open("col_dtypes.yaml"))


def extract_df_from_spreadsheet(gsFileName: str, sheetName: str) -> pd.DataFrame:
    # instantiate Google sheet
    gSheet = CLIENT.open(gsFileName)
    # instantiate worksheet
    worksheet = gSheet.worksheet(sheetName)
    # worksheet to dataframe
    df = pd.DataFrame(worksheet.get_all_records())

    # clean currency data
    df[['Debit Amount', 'Credit Amount']] = df[['Debit Amount', 'Credit Amount']].replace('\$', '', regex=True).replace(',', '', regex=True)
    df = df.astype(dtype = DTYPES)

    return df

def archive_dataframe(df: pd.DataFrame) -> None:
    # save dataframe in "archiveDDMMYYYY" format
    now = datetime.now()
    date = now.strftime("%d%m%Y")
    filename = "statements/archive{}.parquet".format(date)
    df.to_parquet(filename)

def main():
    dfToArchive = extract_df_from_spreadsheet("Personal Finance", "raw_data")
    archive_dataframe(dfToArchive)

if __name__ == "__main__":
    main()