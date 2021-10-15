import gspread, os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def service_acct(cred):
    if os.path.isfile(cred) and os.access(cred, os.R_OK):
        try:
            service_acc = gspread.service_account(filename=cred)
        except:
            service_acc = None

    else:
        service_acc = None
    return service_acc

def get_key(url):
    id = url.split('/')[-2]
    return id


def eda(df):
    df['Date'] = pd.to_datetime(df.Date, infer_datetime_format=True) #covnert date object to date_format
    df.rename(columns={"Daily Active Users": "dau", "Subscription started": "sub_started"}, inplace=True)
    df.set_index('Date', inplace=True)

    #list of the unique dates
    unique_dates = list(df.index.unique())

    #list of the unique networks
    networks = list(df.Network.unique())
    return unique_dates, networks


def main():

    service_acc = service_acct("twigeo-credentials.json")
    sheet_key = get_key("https://docs.google.com/spreadsheets/d/1nRteMJI2lE05AFs4fSTxEvCXnbRLo1dmNjC1xC0IuZU/edit#gid=910743533")
    workbook = service_acc.open_by_key(sheet_key).sheet1
    data = workbook.get_all_records()
    dataframe = pd.DataFrame.from_dict(data)






if __name__ == "__main__":
    main()
