from google.cloud import bigquery
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
import numpy as np
import streamlit as st
import os
import pickle
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit.components.v1 as components  # Added for embedding

# Initialize BigQuery client
client = bigquery.Client()

# File path for caching
CACHE_FILE = "steam_games_cache.pkl"

# Function to fetch data from BigQuery and cache it
def load_data():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'rb') as f:
            df = pickle.load(f)
        st.info("Loaded data from local cache.")
    else:
        query = """
        SELECT * FROM `stellar-river-464405-k3.steam_data.cleaned_steam_games`
        """
        df = client.query(query).to_dataframe()
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(df, f)
        st.info("Fetched data from BigQuery and saved to cache.")
    return df

# Function to clean and combine text features
def clean_text(text):
    if isinstance(text, (list, np.ndarray)):
        text = ' '.join([str(item) for item in text])
    elif isinstance(text, str):
        text = text
    else:
        text = str(text)
    text = re.sub(r'[^\w\s]', '', text.lower())
    return text

# Load and preprocess data
df = load_data()
df['combined_features'] = df['genre'].apply(clean_text) + ' ' + df['tags'].apply(clean_text)

# Create TF-IDF vectorizer and similarity matrix
@st.cache_resource
def compute_similarity_matrix():
    tfidf = TfidfVectorizer(stop_words='english', max_features=5000)
    tfidf_matrix = tfidf.fit_transform(df['combined_features'])
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)
    return cosine_sim

cosine_sim = compute_similarity_matrix()

# Function to get recommendations
def get_recommendations(game_name, cosine_sim=cosine_sim, df=df, top_n=5):
    try:
        idx = df[df['name'].str.lower() == game_name.lower()].index[0]
    except IndexError:
        return f"Game '{game_name}' not found in the dataset."
    
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:top_n+1]
    game_indices = [i[0] for i in sim_scores]
    recommendations = df[['name', 'genre', 'tags']].iloc[game_indices].copy()
    recommendations['similarity_score'] = [i[1] for i in sim_scores]
    return recommendations

# Streamlit UI
def main():
    st.title("Game Recommendation Engine")
    st.write("Select a game to get recommendations for similar games based on genre and tags.")

    # Dropdown to select a game
    game_name = st.selectbox("Choose a game:", df['name'].sort_values().tolist(), index=0)

    # Button to get recommendations
    if st.button("Get Recommendations"):
        recommendations = get_recommendations(game_name, top_n=5)
        
        if isinstance(recommendations, str):
            st.error(recommendations)
        else:
            st.subheader(f"Recommendations for '{game_name}':")
            st.dataframe(
                recommendations,
                column_config={
                    "name": "Game Name",
                    "genre": "Genre",
                    "tags": "Tags",
                    "similarity_score": st.column_config.NumberColumn(
                        "Similarity Score", format="%.3f"
                    )
                },
                hide_index=True
            )

    # Button to show Looker Studio dashboard
    if st.button("Show Data Analysis"):
        st.subheader("Interactive Game Analytics Dashboard")
        components.iframe(
            # "https://lookerstudio.google.com/embed/reporting/iyoo4t95SXs/page/1M",
            "https://lookerstudio.google.com/embed/reporting/9718b5a1-7e85-48cf-abfe-78d10b499953/page/5SQSF",
            height=1000,
            width=1400,
            scrolling=True
        )

if __name__ == "__main__":
    main()
