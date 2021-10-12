import gspread, os


def service_acct(cred):
    if os.path.isfile(cred) and os.access(cred, os.R_OK):
        try:
            service_acc = gspread.service_account(filename=cred)
        except:
            service_acc = None

    else:
        print('You need credentials file to run this app!')
        service_acc = None
    return service_acc


def main():
    credentials = "twigeo-credentials.json"
    service_acc = service_acct(credentials)
    workbook = service_acc.open_by_key("1nRteMJI2lE05AFs4fSTxEvCXnbRLo1dmNjC1xC0IuZU").sheet1
    data = workbook.get_all_records()

    # print(data)


if __name__ == "__main__":
    main()
