import pandas as pd
from openbb import obb
import streamlit as st
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from langchain import OpenAI, SQLDatabase, SQLDatabaseChain
from langchain.callbacks import get_openai_callback


st.set_page_config(page_title="NFT Crypto", page_icon="📈", layout="wide")

st.title("Crypto NFT data analysis with OpenBB:butterfly: & ChatGPT:robot_face:")

options_df = obb.crypto.nft.collections()['slug']

choice = st.selectbox(
    label = "Choose an NFT Slug",
    options = (options_df)
)

coll = obb.crypto.nft.collections()

del coll['imageBlur']

cont = st.container()

with cont:
    @st.cache_data(experimental_allow_widgets=True)
    def data_stream():

        st.write("Choose a Dataset to load")

        col1, col2, col3 = st.columns([1,1,1])

        if 'data' not in st.session_state:
            st.session_state['data'] = coll

        if col1.button('Collections Data'):
            st.session_state['data'] = coll

        if col2.button('NFT Floor Price'):
            st.session_state['data'] = obb.crypto.nft.fp(choice)

        if col3.button('NFT Stats'):
            st.session_state['data'] = obb.crypto.nft.stats(slug = choice)

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
