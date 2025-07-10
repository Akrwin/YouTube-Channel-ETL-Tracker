import streamlit as st
import duckdb
import pandas as pd
import os
import altair as alt

# Set title
st.title("📊 YouTube Channel Dashboard")

# Connect to DuckDB
db_path = os.path.join('data', 'youtube_data.duckdb')
con = duckdb.connect(db_path)

df = con.execute("SELECT * FROM youtube_data").fetchdf()

# Year filter (based on published_at column)
df['published_year'] = pd.to_datetime(df['published_at']).dt.year
year_options = sorted(df['published_year'].unique(), reverse=True)

# st.sidebar.header("🔍 Filter Your Videos")

# # filter
# selected_year = st.sidebar.selectbox("Select a year", year_options)
# search_keyword = st.sidebar.text_input("Search in title")

# Query the data
st.subheader("📈 Channel Data")
try:
    # Apply filters
    filtered_df = df
    # filtered_df = df[
        # (df['published_year'] == '2025') &
        # (df['title'].str.contains(search_keyword, case=False, na=False))
    # ]
    st.dataframe(filtered_df)
except Exception as e:
    st.error(f"❌ Error loading data: {e}")

# --- Display chart for key metrics ---

st.subheader("📈 Channel Statistics Overview")

try: 
    # Convert view_count to numeric
    df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce')
    
    # Top 20 videos
    top_videos = df.sort_values(by='view_count', ascending=False).head(10)

    # Format view count into 0.0M style
    top_videos['view_count_label'] = top_videos['view_count'].apply(
        lambda x: f"{x/1_000_000:.1f}M" if x >= 1_000_000 else f"{x/1_000:.0f}K"
    )

    # Horizontal bar chart
    bars = alt.Chart(top_videos).mark_bar().encode(
        y=alt.Y('title:N', sort='-x', axis=alt.Axis(labelLimit=300)),
        x=alt.X('view_count:Q'),
        tooltip=[
            alt.Tooltip('title:N', title='Video Title'),
            alt.Tooltip('view_count:Q', title='Views', format=',')
        ]

    )

    # Add text at end of bars
    text = alt.Chart(top_videos).mark_text(
        align='left',
        baseline='middle',
        dx=5,
        color = 'blue'  # space between end of bar and text
    ).encode(
        y=alt.Y('title:N', sort='-x'),
        x=alt.X('view_count:Q'),
        text='view_count_label'
    )

    # Combine chart
    chart = (bars + text).properties(
        width=800,
        height=600,
        title='Top 10 Videos by Views (with Labels)'
    )

    st.altair_chart(chart)

except Exception as e :
    st.warning(f"⚠️ Could not generate chart: {e}")


# --- Display chart for key metrics ---

try: 
    # Convert like_count to numeric
    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce')
    
    # Top 20 videos
    top_videos = df.sort_values(by='like_count', ascending=False).head(10)

    # Format view count into 0.0M style
    top_videos['like_count_label'] = top_videos['like_count'].apply(
        lambda x: f"{x/1_000_000:.1f}M" if x >= 1_000_000 else f"{x/1_000:.0f}K"
    )

    # Horizontal bar chart
    bars = alt.Chart(top_videos).mark_bar().encode(
        y=alt.Y('title:N', sort='-x', axis=alt.Axis(labelLimit=300)),
        x=alt.X('like_count:Q'),
        tooltip=[
            alt.Tooltip('title:N', title='Video Title'),
            alt.Tooltip('like_count:Q', title='Likes', format=',')
        ]
    )

    # Add text at end of bars
    text = alt.Chart(top_videos).mark_text(
        align='left',
        baseline='middle',
        dx=5,
        color = 'blue'  # space between end of bar and text
    ).encode(
        y=alt.Y('title:N', sort='-x'),
        x=alt.X('like_count:Q'),
        text='like_count_label'
    )

    # Combine chart
    chart = (bars + text).properties(
        width=800,
        height=600,
        title='Top 10 Videos by Likes (with Labels)'
    )

    st.altair_chart(chart)

except Exception as e :
    st.warning(f"⚠️ Could not generate chart: {e}")

# --- Display chart for key metrics ---

try: 
    # Convert like_count to numeric
    df['comment_count'] = pd.to_numeric(df['comment_count'], errors='coerce')
    
    # Top 20 videos
    top_videos = df.sort_values(by='comment_count', ascending=False).head(10)

    # Format view count into 0.0M style
    top_videos['comment_count_label'] = top_videos['comment_count'].apply(
        lambda x: f"{x/1_000_000:.1f}M" if x >= 1_000_000 else f"{x/1_000:.0f}K"
    )

    # Horizontal bar chart
    bars = alt.Chart(top_videos).mark_bar().encode(
        y=alt.Y('title:N', sort='-x', axis=alt.Axis(labelLimit=300)),
        x=alt.X('comment_count:Q'),
        tooltip=[
            alt.Tooltip('title:N', title='Video Title'),
            alt.Tooltip('like_count:Q', title='Likes', format=',')
        ]
    )

    # Add text at end of bars
    text = alt.Chart(top_videos).mark_text(
        align='left',
        baseline='middle',
        dx=5,
        color = 'blue'  # space between end of bar and text
    ).encode(
        y=alt.Y('title:N', sort='-x'),
        x=alt.X('comment_count:Q'),
        text='comment_count_label'
    )

    # Combine chart
    chart = (bars + text).properties(
        width=800,
        height=600,
        title='Top 10 Videos by Comments (with Labels)'
    )

    st.altair_chart(chart)

except Exception as e :
    st.warning(f"⚠️ Could not generate chart: {e}")