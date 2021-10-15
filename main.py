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


def network_dau(dates, networks, df):
    network_daily_user = []
    for date in dates:
        d = df[df.index == date] #data frame for each day
        network_daily_user.append({i: d[d.Network == i].dau.sum() for i in networks}) #dau for each network
    network_daily_user = pd.DataFrame.from_dict(network_daily_user)
    network_daily_user.index = dates
    return network_daily_user

def plot_it(df):
    network = df.eq(df.max(1), axis=0).dot(df.columns)[1] #the network that has the max value
    sns.set_style("darkgrid")
    plt.figure(figsize=(12, 5))
    sns.lineplot(data=df).set_title(f"{network} usually has the most active users on a daily basis")


def main():

    service_acc = service_acct("twigeo-credentials.json")
    sheet_key = get_key("https://docs.google.com/spreadsheets/d/1nRteMJI2lE05AFs4fSTxEvCXnbRLo1dmNjC1xC0IuZU/edit#gid=910743533")
    workbook = service_acc.open_by_key(sheet_key).sheet1
    data = workbook.get_all_records()
    print('fetch data from the sheet... Done!')
    dataframe = pd.DataFrame.from_dict(data)

    unique_dates, networks = eda(dataframe)
    #create new dataframe for each unique day
    net_dau = network_dau(unique_dates, networks, dataframe)
    plot_it(net_dau)







if __name__ == "__main__":
    main()
