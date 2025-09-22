import dash
from dash import dcc, html, Input, Output, State, dash_table
import plotly.express as px
import pandas as pd
import os
from google.transit import gtfs_realtime_pb2
import pytz

file_path = "received_files/vehicle_positions.pb"
gtfs_static_folder = "GFTS-Static"
file_paths = {
    "stop_times_df": "stop_times.txt",
    "routes_df": "routes.txt",
    "stops_df": "stops.txt",
    "trips_df": "trips.txt",
    "shapes_df": "shapes.txt"
}
def parse_vehicle_positions(data: bytes) -> pd.DataFrame:
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(data)
    except Exception as e:
        print(f"Błąd parsowania vehicle_positions: {e}")
        return pd.DataFrame()
    return pd.DataFrame([{
        "trip_id": v.trip.trip_id,
        "schedule_relationship": v.trip.schedule_relationship,
        "route_id": v.trip.route_id,
        "latitude": v.position.latitude,
        "longitude": v.position.longitude,
        "current_stop_sequence": v.current_stop_sequence,
        "timestamp": v.timestamp,
        "vehicle_id": v.vehicle.id,
        "vehicle_label": v.vehicle.label,
        "vehicle_type": "bus" if (str(v.trip.route_id).isdigit() and int(v.trip.route_id) >= 100) else "tram" if (str(v.trip.route_id).isdigit() and int(v.trip.route_id) < 100) else "unknown"
    } for entity in feed.entity if entity.HasField("vehicle") for v in [entity.vehicle]])

def load_static_files():
    return {
        key: pd.read_csv(os.path.join(gtfs_static_folder, fname)) if os.path.exists(os.path.join(gtfs_static_folder, fname)) else pd.DataFrame()
        for key, fname in file_paths.items()
    }

def load_vehicle_positions():
    if not os.path.exists(file_path):
        return pd.DataFrame()
    with open(file_path, "rb") as f:
        return parse_vehicle_positions(f.read())
def create_map_figure(df):
    if df.empty:
        return px.scatter_map().update_layout(mapbox_style="open-street-map")

    color_discrete_map = {"tram": "#0000FF", "bus": "#FFA500", "unknown": "#808080"}


    fig = px.scatter_map(
        df,
        lat="latitude",
        lon="longitude",
        text="route_id",
        hover_name="vehicle_id",
        hover_data=["route_id", "trip_id"],
        zoom=12,
        height=600,
        color="vehicle_type",
        color_discrete_map=color_discrete_map,
        category_orders={"vehicle_type": ["bus", "tram", "unknown"]}
    )
    fig.update_layout(mapbox_style="open-street-map", margin={"r":0, "t":0, "l":0, "b":0})
    fig.update_traces(marker=dict(size=12), textposition='top center')

    # Możesz edytować layout legendy np. tytuł:
    fig.update_layout(legend_title_text='Vehicle type')

    return fig

def calculate_avg_delay(df, stop_times_df):
    if df.empty or stop_times_df.empty:
        return pd.DataFrame(columns=["route_id", "delay_sec"])

    merged = pd.merge(df, stop_times_df, left_on=['trip_id', 'current_stop_sequence'],
                      right_on=['trip_id', 'stop_sequence'], how='inner')

    def time_to_sec(t):
        try:
            h, m, s = map(int, str(t).split(':'))
            return h * 3600 + m * 60 + s
        except:
            return 0

    merged['scheduled_sec'] = merged['departure_time'].apply(time_to_sec)

    def utc_to_cet_seconds(ts):
        cet = pytz.timezone('Europe/Warsaw')
        utc_times = pd.to_datetime(ts, unit='s', utc=True)
        cet_times = utc_times.dt.tz_convert(cet)
        return cet_times.dt.hour * 3600 + cet_times.dt.minute * 60 + cet_times.dt.second

    merged['actual_sec'] = utc_to_cet_seconds(merged['timestamp'])
    merged['delay_sec'] = merged['actual_sec'] - merged['scheduled_sec']

    avg_delay = merged.groupby('route_id')['delay_sec'].mean().dropna().reset_index()
    avg_delay['delay_sec'] = pd.to_numeric(avg_delay['delay_sec'], errors="coerce").round(1)

    return avg_delay
def create_app():
    app = dash.Dash(__name__)
    static_data = load_static_files()

    # Adjust these constants to easily fine-tune all cards
    CARD_HEIGHT = '60px'          # Changed from 110px
    CARD_FONT_SIZE = '22px'       # Changed from 28px
    CARD_PADDING = '10px 10px'    # Changed from '23px 10px'
    CARD_MARGIN_BOTTOM = '15px'   # Changed from '20px'
    SMALL_TEXT_FONT_SIZE = '12px' # Font size for small textbox below cards

    app.layout = html.Div([
        html.H1(
            "Paweł Rotnicki's real-time Poznań bus and trams tracker",
            style={'fontSize': '36px', 'textAlign': 'center', 'marginBottom': '20px'}
        ),
        html.Div([
            # Left column with 3 cards and table
            html.Div([
                # Vehicle count card
                html.Div(
                    id='vehicle-count-card',
                    style={
                        'width': '98%',
                        'height': CARD_HEIGHT,               # <- Use adjusted height
                        'marginBottom': CARD_MARGIN_BOTTOM, # <- Use adjusted margin bottom
                        'padding': CARD_PADDING,             # <- Use adjusted padding
                        'border': '1px solid #ccc',
                        'borderRadius': '10px',
                        'fontSize': CARD_FONT_SIZE,          # <- Use adjusted font size
                        'fontWeight': '700',
                        'fontFamily': 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif',
                        'backgroundColor': '#fefefe',
                        'textAlign': 'center',
                        'boxShadow': '4px 4px 15px rgba(0,0,0,0.15)',
                        'color': '#333',
                    }
                ),

                # Total delay sum card
                html.Div(
                    id='global-delay-card',
                    style={
                        'width': '98%',
                        'height': CARD_HEIGHT,
                        'marginBottom': CARD_MARGIN_BOTTOM,
                        'padding': CARD_PADDING,
                        'border': '1px solid #ccc',
                        'borderRadius': '10px',
                        'fontSize': CARD_FONT_SIZE,
                        'fontWeight': '700',
                        'fontFamily': 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif',
                        'backgroundColor': '#fefefe',
                        'textAlign': 'center',
                        'boxShadow': '4px 4px 15px rgba(0,0,0,0.15)',
                        'color': '#333',
                    }
                ),

                # Avg delay per vehicle card
                html.Div(
                    id='avg-delay-card',
                    style={
                        'width': '98%',
                        'height': CARD_HEIGHT,
                        'marginBottom': CARD_MARGIN_BOTTOM,
                        'padding': CARD_PADDING,
                        'border': '1px solid #ccc',
                        'borderRadius': '10px',
                        'fontSize': CARD_FONT_SIZE,
                        'fontWeight': '700',
                        'fontFamily': 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif',
                        'backgroundColor': '#fefefe',
                        'textAlign': 'center',
                        'boxShadow': '4px 4px 15px rgba(0,0,0,0.15)',
                        'color': '#333',
                    }
                ),

                # Small textbox below third card
                html.Div(
                    "* Negative delay means the vehicle is ahead of schedule.",
                    style={
                        'fontSize': SMALL_TEXT_FONT_SIZE,
                        'color': '#666',
                        'marginBottom': '20px',
                        'marginTop': '-8px',  # slight negative margin pulls it closer to cards
                        'fontFamily': 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif',
                        'textAlign': 'center'
                    }
                ),

                # Data table
                dash_table.DataTable(
                    id='delay-table',
                    columns=[
                        {"name": "Route", "id": "route_id", "type": "numeric"},
                        {"name": "Average delay (sec)", "id": "delay_sec", "type": "numeric"}
                    ],
                    sort_action='native',
                    sort_by=[{"column_id": "route_id", "direction": "asc"}],
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontSize': '18px',
                                'fontFamily': 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif'},
                    style_cell_conditional=[
                        {'if': {'column_id': 'route_id'}, 'width': '40%'},
                        {'if': {'column_id': 'delay_sec'}, 'width': '30%'}
                    ],
                    style_header={'fontWeight': 'bold', 'fontSize': '20px', 'backgroundColor': '#f0f0f0',
                                  'borderBottom': '2px solid #ddd'},
                    style_table={
                        'width': '100%',
                        'height': '525px',
                        'overflowY': 'auto',
                        'borderRadius': '8px',
                        'boxShadow': '3px 3px 10px rgba(0,0,0,0.1)',
                        'margin': '0 auto'
                    },
                    style_data={'fontSize': '16px'},
                    style_data_conditional=[
                        {'if': {'row_index': 'odd'}, 'backgroundColor': '#fafafa'}
                    ]
                )
            ], style={
                'flex': '1',
                'minWidth': '370px',
                'maxWidth': '450px',
                'display': 'flex',
                'flexDirection': 'column',
                'justifyContent': 'flex-start',
                'height': '1280px',  # Adjusted for smaller cards + small textbox + table
                'marginRight': '40px'
            }),

            # Right column with map and text box
            html.Div([
                dcc.Graph(id='live-map', style={'height': '660px', 'width': '100%', 'marginBottom': '1px'}),  # Reduced marginBottom from 20px to 10px
                html.Div([
                    html.P("Hi! My name is Paweł, and welcome to my portfolio project. In this project, I connected to publicly available live data from ZTM Poznań, my city's public transport provider, which is stored in .pb format. This format is a binary standard for GTFS real-time data. To unpack it, I studied Google’s documentation and wrote a custom parsing function to convert .pb files to CSV."),
                    html.Br(),
                    html.P([
                        html.B("Tech stack:"),
                        " Kafka (for streaming new files), Docker (for hosting Kafka and Zookeeper), Pandas (for data conversion, cleaning, and joining tables to calculate vehicle delays), Plotly (for data visualizations, including the live map), and Dash (for the interactive browser dashboard)."
                    ]),
                    html.Br(),
                    html.P([
                        html.B("What I learned:"),
                        " Working with Kafka for real-time data, setting up Docker, basic HTML for Dash visuals, and handling incomplete data sets using Pandas (I previously worked mostly with Azure Data Factory and SQL scripts)."
                    ]),
                    html.Br(),
                    html.P([
                        html.B("Unexpected challenges:"),
                        " Choosing the right visualization tool—originally, I tried Streamlit, but it became slow during refreshes; managing refreshes for both live and static data separately; and discovering that large delays were caused by differences in time zones between live and static data."
                    ]),
                    html.Br(),
                    html.P([
                        html.B("Contact:"),
                        " ",
                        html.A("pawel.rotnicki@gmail.com", href="mailto:pawel.rotnicki@gmail.com"),
                        " | GitHub:"
                    ])
                ], style={
                    'height': '200px',
                    'border': '1px solid #ccc',
                    'borderRadius': '10px',
                    'padding': '10px',
                    'fontSize': '17px',
                    'fontFamily': 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif',
                    'backgroundColor': '#fefefe',
                    'boxShadow': '4px 4px 15px rgba(0,0,0,0.12)',
                    'color': '#444',
                    'textAlign': 'center',
                    'overflowY': 'auto'
                })
            ], style={
                'flex': '1',
                'display': 'flex',
                'flexDirection': 'column',
                'justifyContent': 'flex-start',
                'height': '970px'
            })
        ], style={
            'display': 'flex',
            'flexDirection': 'row',
            'justifyContent': 'center',
            'alignItems': 'flex-start',
            'width': '100%',
            'gap': '32px',
            'maxWidth': '1700px',
            'margin': 'auto',
            'paddingTop': '22px'
        }),

        dcc.Interval(id='interval-component', interval=5000, n_intervals=0),
        dcc.Store(id='map-state', data={})
    ])

    def format_seconds_to_hms(seconds):
        negative = seconds < 0
        seconds = abs(int(round(seconds)))
        hours = seconds // 3600
        remainder = seconds % 3600
        minutes = remainder // 60
        secs = remainder % 60

        if hours > 0:
            result = f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            result = f"{minutes}m {secs}s"
        else:
            result = f"{secs}s"

        return f"-{result}" if negative else result

    @app.callback(
        Output('live-map', 'figure'),
        Output('map-state', 'data'),
        Output('delay-table', 'data'),
        Output('global-delay-card', 'children'),
        Output('avg-delay-card', 'children'),
        Output('vehicle-count-card', 'children'),
        Input('interval-component', 'n_intervals'),
        Input('live-map', 'relayoutData'),
        State('map-state', 'data')
    )
    def update_map_and_delay(n_intervals, relayout_data, stored_state):
        if relayout_data:
            center = relayout_data.get('mapbox.center')
            zoom = relayout_data.get('mapbox.zoom')
            if center and zoom:
                stored_state = {'center': center, 'zoom': zoom}

        df = load_vehicle_positions()
        fig = create_map_figure(df)

        if stored_state.get('center') and stored_state.get('zoom'):
            fig.update_layout(mapbox=dict(center=stored_state['center'], zoom=stored_state['zoom']))

        avg_delay = calculate_avg_delay(df, static_data["stop_times_df"])
        if not avg_delay.empty:
            avg_delay['route_id'] = pd.to_numeric(avg_delay['route_id'], errors='coerce').fillna(0).astype(int)
        delay_data = avg_delay.to_dict('records') if not avg_delay.empty else []

        global_delay_sum = avg_delay['delay_sec'].sum() if not avg_delay.empty else 0
        global_delay_text = f"Total delay sum:\n\n\n{format_seconds_to_hms(global_delay_sum)}*"

        vehicle_count = df['vehicle_id'].nunique() if not df.empty else 0
        avg_delay_per_vehicle = (global_delay_sum / vehicle_count) if vehicle_count > 0 else 0
        avg_delay_text = f"Avg delay per vehicle:\n\n\n{format_seconds_to_hms(avg_delay_per_vehicle)}*"

        vehicle_count_text = f"Total vehicles live:\n\n\n{vehicle_count}"

        return fig, stored_state, delay_data, global_delay_text, avg_delay_text, vehicle_count_text

    return app

if __name__ == "__main__":
    create_app().run(debug=True)
