import gspread, os
import pandas as pd
import plotly.express as px


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
    sheet_key = url.split('/')[-2]
    return sheet_key


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
        d = df[df.index == date] #dataframe for each day
        network_daily_user.append({i: d[d.Network == i].dau.sum() for i in networks}) #dau for each network
    network_daily_user = pd.DataFrame(network_daily_user) # from dict
    network_daily_user.index = dates
    return network_daily_user

def plot_dau(df):
    network = df.eq(df.max(1), axis=0).dot(df.columns)[1] #the network that has the max value
    fig = px.line(df, title=f"{network} has the most active users on a daily basis")
    fig.show()

def plot_ratio(df):
    fig = px.line(df, title="The conversion rate over time")
    fig.show()

def conver_rate(dates, networks, df):
    idf = df[df['Installs'] != 0] #sub dataframe (which installs !=0) con_rate = sub_start/installs
    sub_df = []
    inst_df = []
    for date in dates:
        d = idf[idf.index == date]  # return dataframe for each day

        sub_df.append({i: d[d.Network == i].sub_started.sum() for i in networks}) #agg subStarted for each network in a day
        inst_df.append({i: d[d.Network == i].Installs.sum() for i in networks})   #agg installs for each network in a day
    ssdf = pd.DataFrame(sub_df)
    indf = pd.DataFrame(inst_df)
    ssdf.index = dates  # add dates as index to subscription started dataframe
    indf.index = dates  # add dates as index to installs dataframe
    convRate_df = ssdf.div(indf) # calculate the ratio
    return convRate_df


def main():
    print('fetching data from the sheet...')
    service_acc = service_acct("twigeo-credentials.json")
    sheet_key = get_key("https://docs.google.com/spreadsheets/d/1nRteMJI2lE05AFs4fSTxEvCXnbRLo1dmNjC1xC0IuZU/edit#gid=910743533")
    workbook = service_acc.open_by_key(sheet_key).sheet1
    data = workbook.get_all_records()

    print("Transforming data...")
    dataframe = pd.DataFrame.from_dict(data)

    unique_dates, networks = eda(dataframe)

    #Create first metric "The network usually has the most active users on a daily basis"
    net_dau = network_dau(unique_dates, networks, dataframe)
    plot_dau(net_dau)

    #Create second metric "The network that has the best conversion ratio over time"
    conversion_rate = conver_rate(unique_dates, networks, dataframe)
    plot_ratio(conversion_rate)

    print("# Upload metrics to MySQL..")







if __name__ == "__main__":
    main()
