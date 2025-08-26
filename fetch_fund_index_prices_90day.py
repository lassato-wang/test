import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# List of fund and index codes
codes = [
    '000015', '000016', '000072', '000688', '000805', '000823', '000827', '000828', '000841',
    '000852', '000853', '000913', '000966', '000971', '000974', '000990', '000998', '159755',
    '159766', '159867', '159869', '159996', '160211', '399002', '399005', '399006', '399008',
    '399284', '399295', '399300', '399330', '399393', '399394', '399395', '399396', '399412',
    '399429', '399440', '399441', '399550', '399610', '399632', '399673', '399707', '399803',
    '399804', '399805', '399807', '399808', '399809', '399810', '399811', '399812', '399814',
    '399903', '399904', '399905', '399932', '399934', '399935', '399965', '399966', '399967',
    '399970', '399971', '399973', '399974', '399975', '399976', '399982', '399983', '399986',
    '399987', '399989', '399991', '399992', '399993', '399994', '399995', '399996', '399997',
    '399998', '510900', '511010', '512480', '513060', '513090', '513180', '513330', '513550',
    '513660', '515000', '515050', '515120', '516110', '516780', '516880', '518880', '563000',
    '588000', '980017'
]

# 设置 end_date 为当天
end_date = datetime.today().date()
start_date = end_date - timedelta(days=122)  # Fetch extra days to ensure 90 trading days
start_date_str = start_date.strftime('%Y%m%d')
end_date_str = end_date.strftime('%Y%m%d')

# Initialize DataFrame to store closing prices
dates = []
all_data = {}

# Fetch data for each code
for code in codes:
    try:
        # Try fetching as fund data
        try:
            df_fund = ak.fund_etf_hist_em(symbol=code, period='daily', start_date=start_date_str, end_date=end_date_str, adjust='')
            if not df_fund.empty:
                df_fund['date'] = pd.to_datetime(df_fund['日期']).dt.strftime('%Y-%m-%d')
                all_data[code] = df_fund.set_index('date')['收盘'].to_dict()
                if not dates:
                    dates = df_fund['date'].tolist()[-90:]  # Get last 90 trading days
                continue
        except:
            pass

        # Try fetching as index data
        try:
            # Format code for index (e.g., 'sh000015' or 'sz399300')
            if code.startswith('0') or code.startswith('1'):
                index_code = 'sh' + code
            elif code.startswith('3') or code.startswith('5'):
                index_code = 'sz' + code
            else:
                index_code = code
            df_index = ak.index_zh_a_hist(symbol=index_code, period='daily', start_date=start_date_str, end_date=end_date_str)
            if not df_index.empty:
                df_index['date'] = pd.to_datetime(df_index['日期']).dt.strftime('%Y-%m-%d')
                all_data[code] = df_index.set_index('date')['收盘'].to_dict()
                if not dates:
                    dates = df_index['date'].tolist()[-90:]  # Get last 90 trading days
        except:
            print(f"Data not found for {code}")
            continue

    except Exception as e:
        print(f"Error fetching data for {code}: {e}")
        continue

# Create DataFrame for Excel output
if dates:
    # Ensure we have exactly 90 trading days
    dates = sorted(dates)[-90:]
    result_df = pd.DataFrame(index=dates, columns=codes)

    # Populate DataFrame with closing prices
    for code in codes:
        if code in all_data:
            for date in dates:
                if date in all_data[code]:
                    result_df.loc[date, code] = all_data[code][date]
                else:
                    result_df.loc[date, code] = None  # Missing data

    # Save to Excel
    output_file = 'fund_index_closing_prices_90days.xlsx'
    result_df.to_excel(output_file, index_label='Date')
    print(f"Data saved to {output_file}")
else:
    print("No data retrieved for any codes.")