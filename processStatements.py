import pandas as pd
import numpy as np
import os
import gspread
from pyparsing import col
import yaml

from oauth2client.service_account import ServiceAccountCredentials

# define the scope
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# add credentials to the account
CREDS = ServiceAccountCredentials.from_json_keyfile_name('misc/personalfinance-347102-e8927945dc1c.json', SCOPE)
# authorize the client
CLIENT = gspread.authorize(CREDS)

# load column dtypes file to dict 
DTYPES = yaml.safe_load(open("col_dtypes.yaml"))

# Reads bank statement from a designated file path and returns a concatenated dataframe.
def read_statements(path : str) -> pd.DataFrame:

    fileNames = next(os.walk(path))[2]  # returns files of a directory in a list
    statementFiles = [f for f in fileNames if f.endswith('.csv') or f.endswith('.parquet')]

    csvHeaders = ['Transaction Date', 'Reference', "Debit Amount", "Credit Amount", "Transaction Ref1", "Transaction Ref2", "Transaction Ref3", "spacer"]
    colNames = ['Transaction Date', "Debit Amount", "Credit Amount", "Transaction Ref1", "Transaction Ref2", "Transaction Ref3"]
    par_colNames = ['Transaction Date', "Debit Amount", "Credit Amount", "Transaction Ref1", "Transaction Ref2", "Transaction Ref3", "Category"]

    dfList = []

    for file in statementFiles:
        filePath = path + "/" + file
        if file.endswith('.csv'):
            df = pd.read_csv(filePath, skiprows=20, header=None, names=csvHeaders, usecols=colNames)
            df["Category"] = np.nan
        if file.endswith('.parquet'):
            df = pd.read_parquet(filePath, columns=par_colNames)
        dfList.append(df)
        
    return pd.concat(dfList)
    
def clean_statements(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna('')
    # Typing for monetary fields
    df[["Debit Amount", "Credit Amount"]] = df[["Debit Amount", "Credit Amount"]].replace(' ', '0')
    df = df.astype(dtype = DTYPES)
    # Clean nets transactions
    df["Transaction Ref1"] = np.where(df["Transaction Ref1"] == "28776419", df["Transaction Ref2"], df["Transaction Ref1"])
    df["Transaction Ref1"] = df["Transaction Ref1"].str.lower()

    return df

# Output clean dataset to excel file for analysis and visualization
def output_to_spreadsheet(df : pd.DataFrame) -> None:
    # get the instance of the Spreadsheet
    sheet = CLIENT.open('Personal Finance')
    # get the second sheet of the spreadsheet (raw_data sheet)
    dataSheet = sheet.worksheet("raw_data")

    # Update raw_data sheet in gsheets
    dataSheet.update([df.columns.values.tolist()] + df.values.tolist())

def main():
    statementDir = os.getcwd() + "/statements"
    extractedStatements = read_statements(statementDir)
    cleanedStatements = clean_statements(extractedStatements)
    output_to_spreadsheet(cleanedStatements)

if __name__ == "__main__":
    main()
