import time
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from datetime import datetime
from datetime import timedelta
from selenium.webdriver.support.select import Select
import pandas as pd
import numpy as np

def get_data_pandas(start_str,end_str):
    opts = Options()
    browser = Firefox(options=opts)
    browser.get('https://www.moneycontrol.com/stocks/histstock.php?classic=true')

    time.sleep(1)

    company_name = "Infosys"
    search_form = browser.find_element_by_xpath('//*[@id="mycomp"]')
    search_form.send_keys(company_name)

    time.sleep(5)

    sel = browser.find_element_by_css_selector('#suggest > ul:nth-child(1) > li:nth-child(1) > a:nth-child(1)')
    sel.click()

    time.sleep(1)

    sel = Select(browser.find_element_by_xpath('//*[@id="ex"]'))
    sel.select_by_visible_text('NSE')

    start_date = datetime.strptime(start_str, "%d-%m-%Y")
    end_date = datetime.strptime(end_str, "%d-%m-%Y")

    d = Select(browser.find_element_by_name('frm_dy'))
    d.select_by_visible_text(start_date.strftime("%d"))

    if int(start_date.strftime("%m")) != 6 and int(start_date.strftime("%m")) != 7 :
        m = Select(browser.find_element_by_name('frm_mth'))
        m.select_by_visible_text(start_date.strftime("%b"))
    else:
        m = Select(browser.find_element_by_name('frm_mth'))
        m.select_by_visible_text(start_date.strftime("%B"))

    y = Select(browser.find_element_by_name('frm_yr'))
    y.select_by_visible_text(start_date.strftime("%Y"))

    d = Select(browser.find_element_by_name('to_dy'))
    d.select_by_visible_text(end_date.strftime("%d"))

    if int(start_date.strftime("%m")) != 6 and int(start_date.strftime("%m")) != 7:

        m = Select(browser.find_element_by_name('to_mth'))
        m.select_by_visible_text(end_date.strftime("%b"))
    else:
        m = Select(browser.find_element_by_name('to_mth'))
        m.select_by_visible_text(end_date.strftime("%B"))

    y = Select(browser.find_element_by_name('to_yr'))
    y.select_by_visible_text(end_date.strftime("%Y"))

    browser.find_element_by_css_selector('td.PL20:nth-child(1) > form:nth-child(1) > div:nth-child(4) > input:nth-child(4)').click()

    time.sleep(3)

    table_req = browser.find_element_by_class_name("tblchart")
    rows = table_req.find_elements_by_tag_name("tr")
    data_dict = {"Date":[],"Open":[],"High":[],"Low":[],"Close":[],"Volume":[],"SPREAD-high-low":[], "SPREAD-open-close":[]}
    column_names = ["Date","Open","High","Low","Close","Volume", "SPREAD-high-low", "SPREAD-open-close"]
    for row in rows:
        col = row.find_elements_by_tag_name("td")
        col_num = 0
        for c in col:
            if col_num in (1,2,3,4,5,6,7):
                data_dict[column_names[col_num]].append(float(c.text))
            else:
                data_dict[column_names[col_num]].append(c.text)
            col_num += 1
    browser.close()

    df = pd.DataFrame(data_dict)
    del df["SPREAD-high-low"]
    del df["SPREAD-open-close"]
    df = df.iloc[::-1]
    df = df.reset_index(drop=True)
    print("data scrapped from money control")
    df.to_excel("output/table_from_money_control.xlsx")


    return df


def write_to_excel(number_shares_2019, number_shares_2020):

    try:
        excel_sheet = pd.read_excel('local_VWAP_data.xlsx')
        excel_sheet.drop("Unnamed: 0", inplace=True, axis=1)
        print("table received from excel sheet")
        excel_sheet.to_excel("output/table_from_excel_sheet.xlsx")

        last_row = excel_sheet.values[0].tolist()
        start_date = last_row[0]
        date_time_obj = datetime.strptime(start_date, '%d-%m-%Y')
        date_time_obj = date_time_obj + timedelta(days=1)
        start_date = datetime.strftime(date_time_obj,"%d-%m-%Y")

    except:
        start_date = "01-01-2020"
        excel_sheet = pd.DataFrame()
        print("No table found...")
        print("Creating new table")
        excel_sheet['Date'] = []
        excel_sheet['Open'] = []
        excel_sheet['High'] = []
        excel_sheet['Low'] = []
        excel_sheet['Close'] = []
        excel_sheet['Volume'] = []
        excel_sheet['Close Prc * Vol'] = []
        excel_sheet['VWAP-90'] = []
        excel_sheet['PSU-2019'] = []
        excel_sheet['PSU-2020'] = []
        excel_sheet['Total Value'] = []

    excel_sheet["Date"] = pd.to_datetime(excel_sheet["Date"], dayfirst=True)

    end = datetime.now()
    end_date = datetime.strftime(end,"%d-%m-%Y")

    mc_historical = get_data_pandas(start_date, end_date)
    mc_historical["Date"] = pd.to_datetime(mc_historical["Date"], dayfirst=True)
    mc_historical = mc_historical.sort_values(by='Date')
    mc_historical['Close Prc * Vol'] = mc_historical.Volume * mc_historical.Close
    mc_historical['VWAP-90'] = np.nan
    mc_historical['PSU-2019'] = np.nan
    mc_historical['PSU-2020'] = np.nan

    print("Formatted MC table to requirement")
    mc_historical.to_excel("output/mc_historical_data_post_manipulation.xlsx")

    final_df = pd.concat([mc_historical,excel_sheet])
    final_df["Date"] = pd.to_datetime(final_df["Date"])
    final_df = final_df.sort_values(by='Date')
    final_df = final_df.reset_index(drop=True)

    print("Table ready for calculation")
    final_df.to_excel("output/final_table_before_calculation.xlsx")

    def get_vwap(row):
        if int(row.name)>89:
            return final_df.iloc[int(row.name)-90:int(row.name)].sum()['Close Prc * Vol']/final_df.iloc[int(row.name)-90:int(row.name)].sum()['Volume']
        else:
            return np.nan

    final_df['VWAP-90'] = final_df.apply(get_vwap, axis=1)
    final_df['VWAP-90'] = final_df['VWAP-90'].round(2)
    final_df['PSU-2019'] = (final_df['VWAP-90'] - 230) * number_shares_2019
    final_df['PSU-2020'] = (final_df['VWAP-90'] - 251) * number_shares_2020
    final_df['PSU-2019'] = final_df['PSU-2019'].round(0)
    final_df['PSU-2019'] = final_df['PSU-2020'].round(0)
    final_df = final_df.sort_values(by='Date',ascending=False)
    final_df['Date'] = final_df['Date'].dt.strftime('%d-%m-%Y')
    final_df['Total Value'] = final_df['PSU-2019']+ final_df['PSU-2020']
    print("final table sent to excel sheet")
    final_df.to_excel("local_VWAP_data.xlsx")
    final_df.to_excel("output/local_VWAP_data.xlsx")


number_shares_2019 = 100
number_shares_2020 = 100
write_to_excel(number_shares_2019, number_shares_2020)