import pandas as pd
from openbb_terminal.sdk import openbb
import streamlit as st
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from langchain import OpenAI, SQLDatabase, SQLDatabaseChain
from langchain.callbacks import get_openai_callback


st.set_page_config(page_title="Crypto Due Dilligence", page_icon="📈", layout="wide")

st.title("Crypto Due Dilligence with OpenBB:butterfly: & ChatGPT:robot_face:")

options_df = openbb.crypto.onchain.erc20_tokens()

choice = st.selectbox(
    label = "Choose a Crypto",
    options = (options_df['symbol'])
)

cont = st.container()

with cont:
    @st.cache_data(experimental_allow_widgets=True)
    def data_stream():

        st.write("Choose a Dataset to load")

        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1,1,1,1,1,1,1,1])

        if 'data' not in st.session_state:
            st.session_state['data'] = openbb.crypto.dd.basic(choice)

        if col1.button('Basic Info'):
            st.session_state['data'] = openbb.crypto.dd.basic(choice)

        if col2.button('Events'):
            st.session_state['data'] = openbb.crypto.dd.events(choice)

        if col3.button('Exchanges'):
            st.session_state['data'] = openbb.crypto.dd.ex(choice)

        if col4.button('Messari Time Series'):
            st.session_state['data'] = openbb.crypto.dd.get_mt(choice)

        if col5.button('Market Cap Dominance'):
            st.session_state['data'] = openbb.crypto.dd.mcapdom(choice)

        if col6.button('Markets'):
            st.session_state['data'] = openbb.crypto.dd.mkt(choice)

        if col7.button('News'):
            st.session_state['data'] = openbb.crypto.dd.news()

        if col8.button('Twitter'):
            st.session_state['data'] = openbb.crypto.dd.twitter(choice)


data_stream()

data1 = st.session_state['data']
st.write(data1)

table_name = 'statesdb'
uri = "file:memory?cache=shared&mode=memory"
openai_key = st.secrets["OPENAI_KEY"]

query = st.text_input(
    label = "Any questions?",
    help = "Ask any question based on the loaded dataset")

conn = sqlite3.connect(uri, uri=True)


if isinstance(data1, pd.DataFrame):
    if not data1.empty:
        data1.to_sql(table_name, conn, if_exists = "replace", index = False)
    else:
        st.write("No data")

else:
    st.write("Error. Invalid data or failed API connection.")
    
db_eng = create_engine(
    url = 'sqlite:///file:memdb1?mode=memory&cache=shared',
    poolclass = StaticPool,
    creator = lambda: conn
)
db = SQLDatabase(engine = db_eng)

lang_model = OpenAI(
    openai_api_key = openai_key,
    temperature = 0.9,
    max_tokens = 300
)
db_chain = SQLDatabaseChain(llm = lang_model, database = db, verbose = True)


if query:
    with get_openai_callback() as cb:
        response = db_chain.run(query)
        st.sidebar.write("Your request costs: " + str(cb.total_cost) + "USD")
    st.write(response)
