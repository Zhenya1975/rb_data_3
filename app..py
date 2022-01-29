import dash_bootstrap_components as dbc
import dash
from dash import html, dcc
import pandas as pd
import numpy as np
import plotly.express as px
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def load_actions_data():
    origin_csv_file_location = 'data/rb_bmtechnics_ru_2.csv'

    chunk = pd.read_csv(origin_csv_file_location, chunksize=100000)
    origin_df = pd.concat(chunk)
    origin_df.to_parquet('data/rb_bmtechnics_ru_2.parquet')
    origin_df = pd.read_parquet('data/rb_bmtechnics_ru_2.parquet')
    # присвоили id как индекс
    # origin_df.set_index('id', inplace=True)
    # добавляем колонку "В каком году было событие"
    origin_df['created_at_year'] = pd.to_datetime(origin_df['created_at'], unit='s').dt.year

    # добавляем колонку "В каком квартале было событие"
    origin_df['created_at_quarter'] = pd.to_datetime(origin_df['created_at'], unit='s').dt.quarter

    # добавляем колонку "В каком месяце было событие"
    origin_df['created_at_month'] = pd.to_datetime(origin_df['created_at'], unit='s').dt.month
    # добавляем колонку "В какой дате было событие"
    origin_df['created_at_date'] = pd.to_datetime(origin_df['created_at'], unit='s').dt.date
    # добавляем колонку "В каком дне недели было событие"
    origin_df['created_at_weekday'] = pd.to_datetime(origin_df['created_at'], unit='s').dt.dayofweek + 1

    # добавляем колонку "В каком часу было событие"
    origin_df['created_at_hour'] = pd.to_datetime(origin_df['created_at'], unit='s').dt.hour + 3
    # origin_df.to_csv('data/origin_df_delete.csv')
    origin_df.sort_values(['created_at_date'], inplace=True)

    df_2020_2021 = origin_df[origin_df['created_at_year'].isin([2020, 2021])]
    # df_2020_2021.to_csv('data/2020_2021_actions.csv')
    df_2020_2021.to_parquet('data/2020_2021_actions.parquet')


load_actions_data()

# df_2021_2021 = pd.read_csv('data/2020_2021_actions.csv', index_col=0, parse_dates=True)
#
# # # очищаем от строк с пустым "object_id"
# df_2021_2021['object_id'].replace('', np.nan)
# df_2021_human_actions = df_2021_2021.dropna(subset=['object_id'])
# # группируем по дням
# # number_of_actions - считаем количество записей по полю id
# # number_of_unique_users - считаем количество уникальных записей по полю "id юзера" здесь оно называется 'object_id'
#
# df_aggregate_by_date = df_2021_2021.groupby('created_at_date').agg(number_of_actions=('id', pd.Series.count),
#                                                                    number_of_unique_users=(
#                                                                    'object_id', pd.Series.nunique),
#                                                                    year=('created_at_year', pd.Series.unique),
#                                                                    quarter=('created_at_quarter', pd.Series.unique),
#                                                                    month=(
#                                                                    'created_at_month', pd.Series.unique)).reset_index()
#
# df_aggregate_by_date.to_csv('data/2020_2021_actions_aggregate_by-days.csv')
# # print(df_aggregate_by_date)
# # print(df_aggregate_by_date.info())
# # value_list = [2020, 2021]
# # filtered_df = df_aggregate_by_date[df_aggregate_by_date.year.isin(value_list)]['created_at_date']
# # print(filtered_df)
#
# app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
#
# body = html.Div([
#     dbc.Container([
#         html.H3("Активность в системах")
#         , dbc.Row([
#             dbc.Col(width=4,
#                     children=[
#                         html.Div(style={'background-color': '#D8E2EA', 'padding-left': '30px', 'padding-right': '20px',
#                                         'padding-top': '10px'},
#                                  children=[
#                                      html.Div(
#                                          children=[
#                                              html.P('Годы'),
#                                              dcc.Dropdown(id='yearselector',
#                                                           options=[{'label': '2020', 'value': 2020},
#                                                                    {'label': '2021', 'value': 2021}],
#                                                           multi=True,
#                                                           value=[df_aggregate_by_date['year'][0]]
#                                                           ), ]),
#                                  ])
#                     ]),
#             dbc.Col(width=8,
#                     children=[
#                         html.Div(style={'background-color': '#D8E2EA', 'padding-left': '30px', 'padding-right': '20px',
#                                         'padding-top': '10px'},
#                                  children=[dcc.Graph(id='days_activity', config={'displayModeBar': False}, animate=True)
#                                            ])
#
#                     ])
#         ])
#     ])
# ])
# app.layout = html.Div([body])
#
#
# @app.callback(Output('days_activity', 'figure'),
#               [Input('yearselector', 'value')])
# def update_days_activity(selected_dropdown_value):
#     """Создание графика по значениям Value на основании выбранных значений"""
#     # Create figure with secondary y-axis
#     df_sub = df_aggregate_by_date[df_aggregate_by_date.year.isin(selected_dropdown_value)]
#     # df_sub.to_csv('data/df_sub.csv')
#     fig = make_subplots(specs=[[{"secondary_y": True}]])
#     fig.add_trace(
#         go.Scatter(x=df_sub['created_at_date'],
#                    y=df_sub['number_of_actions'],
#                    name='number_of_actions',
#
#                    ),
#
#         secondary_y=False, )
#
#     fig.add_trace(
#         go.Scatter(x=df_sub['created_at_date'],
#                    y=df_sub['number_of_unique_users'],
#                    name="number_of_unique_users"),
#         secondary_y=True, )
#     # Set y-axes titles
#     fig.update_yaxes(
#         title_text="Number of actions",
#         secondary_y=False)
#     fig.update_yaxes(
#         title_text="number of unique_users",
#         secondary_y=True)
#     fig.update_layout(legend=dict(
#         orientation="h",
#         yanchor="bottom",
#         y=1,
#         xanchor="right",
#         x=1
#     ))
#
#     return fig
#
#
#
#
# if __name__ == "__main__":
#     app.run_server(debug=True)
