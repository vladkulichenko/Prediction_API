from fbprophet import Prophet
import holidays
import warnings
from pymongo import MongoClient
import pandas as pd
warnings.filterwarnings('ignore')

holiday = pd.DataFrame([])
for date, name in sorted(holidays.Ukraine(years=[2018,2019,2020,2021]).items()):
    holiday = holiday.append(pd.DataFrame({'ds': date, 'holiday': "UA-Holidays"}, index=[0]), ignore_index=True)
holiday['ds'] = pd.to_datetime(holiday['ds'], format='%Y-%m-%d', errors='ignore')


def get_from_bd(bd_url, db_name, db_table):
    client = MongoClient(bd_url)
    db = client[db_name]
    col = db[db_table]
    df = pd.DataFrame(list(col.find()))
    df.drop('_id', axis=1, inplace=True)
    return df

def post_to_db(df, bd_url, db_name,db_table ):
    client = MongoClient(bd_url)
    db = client[db_name]
    col = db[db_table]
    col.insert_many(df.to_dict('records'))

def dish_predict(dish, df):
    dish_data = df[df['Блюдо'].str.contains(dish, na=False)]
    dish_data["Время открытия"] = pd.to_datetime(dish_data["Время открытия"])
    dish_data['Timestamp'] = dish_data['Время открытия'].apply(lambda x: f'{x.year}-{x.month}-{x.day} {x.hour}:00:00')
    dish_data['Timestamp'] = pd.to_datetime(dish_data['Timestamp'])
    dish_data.drop('Время открытия', axis=1, inplace=True)
    val_count = dish_data['Timestamp'].value_counts()
    dish_data = pd.DataFrame(val_count)
    dish_data.sort_index(inplace=True)
    dish_data.rename(columns = {'Timestamp':'count'}, inplace=True)
    dish_data = dish_data.reset_index()
    dish_data.columns = ['ds', 'y']
    start = pd.to_datetime(str(dish_data['ds'].min()))
    end =  pd.to_datetime(str(dish_data['ds'].max()))
    dates = pd.date_range(start=start, end=end, freq='1H')
    dish_data = dish_data.set_index('ds').reindex(dates).reset_index()
    dish_data.fillna(0, inplace=True)
    dish_data.rename(columns={'index':'ds'}, inplace=True)
    predictions_dish = 120  # на следующие 120 часов
    train_dish = dish_data[:-predictions_dish]
    test_dish = dish_data[:predictions_dish]
    model_dish = Prophet(holidays=holiday,
                        changepoint_prior_scale= 0.1,
                        holidays_prior_scale = 0.5,
                        n_changepoints = 100,
                        seasonality_mode = 'multiplicative',
                        weekly_seasonality=True,
                        daily_seasonality = True,
                        yearly_seasonality = True,
                        interval_width=0.95)
    model_dish.add_country_holidays(country_name='UA')
    model_dish.fit(train_dish)
    future_dish = model_dish.make_future_dataframe(periods=predictions_dish, freq='1H')
    future_dish = model_dish.predict(future_dish)
    # model_dish.plot_components(future_dish)
    # cmp_df_fila = future_dish.set_index('ds')[['yhat', 'yhat_lower', 'yhat_upper']].join(dish_data.set_index('ds'))
    # cmp_df_fila['e'] = cmp_df_fila['y'] - cmp_df_fila['yhat']
    # cmp_df_fila['p'] = 100 * cmp_df_fila['e'] / cmp_df_fila['y']
    # #print('MAE: ', np.mean(abs(cmp_df_fila[-predictions_dish:]['e']))) # Средний модуль отклонения
    return future_dish

