import gspread, os
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine


class ETL_gsheet:
    def __init__(self):
        self.sheet = "https://docs.google.com/spreadsheets/d/1nRteMJI2lE05AFs4fSTxEvCXnbRLo1dmNjC1xC0IuZU/edit#gid=910743533"
        self.mysql_password = 'gammoury16R!'
        self.network_daily_user = [] # the daily active user networks
        self.sub_df = []  # Subscription started
        self.inst_df = []  # installs

    #set up a google service account to read
    def service_acct(self, cred):
        if os.path.isfile(cred) and os.access(cred, os.R_OK):
            try:
                service_acc = gspread.service_account(filename=cred)
            except:
                service_acc = None

        else:
            print("API json key does not exist!")
            service_acc = None
        return service_acc

    def get_key(self, url):
        sheet_key = url.split('/')[-2]
        return sheet_key


    def eda(self, df):
        df['Date'] = pd.to_datetime(df.Date, infer_datetime_format=True) #covnert date object to date_format
        df.rename(columns={"Daily Active Users": "dau", "Subscription started": "sub_started"}, inplace=True)
        df.set_index('Date', inplace=True)

        #list of the unique dates
        unique_dates = list(df.index.unique())

        #list of the unique networks
        networks = list(df.Network.unique())
        return unique_dates, networks


    def network_dau(self, dates, networks, df):
        # network_daily_user = []
        for date in dates:
            d = df[df.index == date] #dataframe for each day
            self.network_daily_user.append({i: d[d.Network == i].dau.sum() for i in networks}) #agg the dau for each network
        network_daily_user = pd.DataFrame(self.network_daily_user)
        network_daily_user.index = dates
        return network_daily_user

    def plot_dau(self, df):
        network = df.eq(df.max(1), axis=0).dot(df.columns)[1] #the network that has the max value
        fig = px.line(df, title=f"{network} has the most active users on a daily basis")
        fig.show()

    def plot_ratio(self, df):
        fig = px.line(df, title="The conversion rate over time")
        fig.show()

    def conver_rate(self, dates, networks, df):
        idf = df[df['Installs'] != 0] #sub dataframe (which installs !=0) con_rate = sub_start/installs
        for date in dates:
            d = idf[idf.index == date]  # return dataframe for each day
            self.sub_df.append({i: d[d.Network == i].sub_started.sum() for i in networks}) #agg subStarted for each network in a day
            self.inst_df.append({i: d[d.Network == i].Installs.sum() for i in networks})   #agg installs for each network in a day
        ssdf = pd.DataFrame(self.sub_df)
        indf = pd.DataFrame(self.inst_df)
        ssdf.index = dates  # add dates as index to subscription started dataframe
        indf.index = dates  # add dates as index to installs dataframe
        convRate_df = ssdf.div(indf) # calculate the ratio
        return convRate_df

    def to_mysql(self, df1, df2):
        my_conn = create_engine(f"mysql+pymysql://root:{self.mysql_password}@localhost/twigeo")
        df1.to_sql(name="dau_metric", con=my_conn, index=False, if_exists='replace')
        df2.to_sql(name="convrate_metric", con=my_conn, index=False, if_exists='replace')

def main():
    gs = ETL_gsheet()
    print('fetching data from the sheet...')
    #create google service account
    service_acc = gs.service_acct("sheetAPI_key.json")

    sheet_key = gs.get_key(gs.sheet)
    try:
        workbook = service_acc.open_by_key(sheet_key).sheet1
        data = workbook.get_all_records()
    except gspread.exceptions.APIError:
        print('The sheet Key is invalid or Not Found ')

    print("Transforming data...")
    dataframe = pd.DataFrame.from_dict(data)

    unique_dates, networks = gs.eda(dataframe)

    #Create first metric "The network usually has the most active users on a daily basis"
    net_dau = gs.network_dau(unique_dates, networks, dataframe)
    gs.plot_dau(net_dau)

    #Create second metric "The network that has the best conversion ratio over time"
    conversion_rate = gs.conver_rate(unique_dates, networks, dataframe)
    gs.plot_ratio(conversion_rate)

    print("Uploading metrics to MySQL..")
    gs.to_mysql(net_dau, conversion_rate)
    print("ETL process successfully finished :-)")


if __name__ == "__main__":
    main()
