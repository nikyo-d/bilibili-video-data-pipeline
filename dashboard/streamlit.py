import streamlit as st
import pandas as pd
import os
import json
import re
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from sklearn.linear_model import LinearRegression
import numpy as np
from config import config
from functools import lru_cache

# --- Config
SNAPSHOT_DIR = config.DAILY_SNAPSHOTS_DIR
TRACKING_POOL_FILE = config.TRACKING_POOL_FILE
REMOVED_VIDEOS_FILE = config.REMOVED_VIDEOS_FILE

# --- Helper Functions
@lru_cache(maxsize=8192)
def extract_bvid_from_link(link):
    if isinstance(link, str):
        match = re.search(r'(BV[0-9A-Za-z]{10})', link)
        if match:
            return match.group(1)
    return None

@st.cache_data(ttl=3600)
def load_snapshots(start_date, end_date):
    all_data = {}
    daily_summaries = []

    for single_date in pd.date_range(start=start_date, end=end_date):
        day_str = single_date.strftime("%Y-%m-%d")
        folder = os.path.join(config.DAILY_SNAPSHOTS_DIR, day_str)

        active_file = os.path.join(folder, f"active_videos_{day_str}.csv")
        if os.path.exists(active_file):
            try:
                df = pd.read_csv(active_file)
                all_data.setdefault("active_videos", []).append((day_str, df))
            except Exception as e:
                st.error(f"Error loading active videos: {str(e)}")

        summary_file = os.path.join(folder, f"summary_{day_str}.txt")
        if os.path.exists(summary_file):
            try:
                with open(summary_file, "r", encoding="utf-8") as f:
                    daily_summaries.append({"date": day_str, "content": f.read()})
            except Exception as e:
                st.error(f"Error loading summary {summary_file}: {str(e)}")

        daily_file = os.path.join(folder, f"daily_videos_{day_str}.csv")
        if os.path.exists(daily_file):
            try:
                df = pd.read_csv(daily_file)
                all_data.setdefault("daily", []).append((day_str, df))
            except Exception as e:
                st.error(f"Error loading {daily_file}: {str(e)}")

        new_videos_file = os.path.join(folder, f"new_videos_{day_str}.csv")
        if os.path.exists(new_videos_file):
            try:
                df = pd.read_csv(new_videos_file)
                all_data.setdefault("new_videos", []).append((day_str, df))
            except Exception as e:
                st.error(f"Error loading new videos: {str(e)}")

        removed_file = os.path.join(folder, f"removed_videos_{day_str}.csv")
        if os.path.exists(removed_file):
            try:
                df = pd.read_csv(removed_file)
                all_data.setdefault("removed", []).append((day_str, df))
            except Exception as e:
                st.error(f"Error loading removed videos: {str(e)}")

    all_data["daily_summaries"] = daily_summaries

    if "active_videos" in all_data:
        engagement_metrics = ["Views", "Likes", "Shares", "Favorites", "Coins", "Comments", "Danmaku"]
        engagement_summary = []

        for day_str, df in all_data["active_videos"]:
            if not isinstance(df, pd.DataFrame):
                continue
            record = {"Date": pd.to_datetime(day_str)}
            for metric in engagement_metrics:
                record[metric] = df[metric].sum() if metric in df.columns else 0
            engagement_summary.append(record)

        all_data["engagement"] = pd.DataFrame(engagement_summary).sort_values("Date")

    return all_data

@st.cache_data
def load_removed_videos_info():
    if os.path.exists(REMOVED_VIDEOS_FILE):
        try:
            df = pd.read_csv(REMOVED_VIDEOS_FILE, on_bad_lines='skip', engine='python')
            return df
        except Exception as e:
            st.error(f"Error loading removed videos info: {str(e)}")
    return pd.DataFrame()

def plot_lineplot(df, x, y, title, x_title="Date", y_title="Count", predict_days=0):
    df = df.copy()
    df[x] = pd.to_datetime(df[x]).dt.date 

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x],
        y=df[y],
        mode='lines+markers',
        name='Actual',
        line=dict(width=2, color='#1f77b4') 
    ))

    if predict_days and len(df) >= 3:
        df = df[[x, y]].dropna().reset_index(drop=True)
        df['Day_Num'] = range(len(df))
        X = df[['Day_Num']]
        y_vals = df[y]

        try:
            model = LinearRegression()
            model.fit(X, y_vals)

            future_X = pd.DataFrame({'Day_Num': range(len(df), len(df) + predict_days)})
            future_dates = [df[x].iloc[-1] + timedelta(days=i + 1) for i in range(predict_days)]
            future_y = model.predict(future_X)

            full_pred_dates = [df[x].iloc[-1]] + future_dates
            full_pred_y = [df[y].iloc[-1]] + list(future_y)

            fig.add_trace(go.Scatter(
                x=full_pred_dates,
                y=full_pred_y,
                mode='lines+markers',
                name=f"Prediction +{predict_days}d",
                line=dict(dash='dot', color='#ff7f0e') 
            ))
        except Exception as e:
            st.warning(f"Prediction failed: {e}")

    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_title,
        hovermode="x unified",
        xaxis_tickformat="%Y-%m-%d",
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=60, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2a3f5f')
    )
    return fig

def calculate_growth(df, metric, days):
    if len(df) < days + 1:
        return None
    current = df[metric].iloc[-1]
    previous = df[metric].iloc[-days-1]
    return (current - previous) / previous * 100 if previous != 0 else 0

def show_growth_metrics(df, metric):
    if len(df) < 2:
        return
    
    cols = st.columns(3)
    with cols[0]:
        growth = calculate_growth(df, metric, 1)
        st.metric("1-Day Growth", f"{growth:.1f}%" if growth is not None else "N/A")
    
    with cols[1]:
        growth = calculate_growth(df, metric, 3) if len(df) >= 4 else None
        st.metric("3-Day Growth", f"{growth:.1f}%" if growth is not None else "N/A")
    
    with cols[2]:
        growth = calculate_growth(df, metric, 7) if len(df) >= 8 else None
        st.metric("7-Day Growth", f"{growth:.1f}%" if growth is not None else "N/A")

def plot_horizontal_bar(df, x, y, title, total=None):
    df = df.sort_values(x, ascending=True)
    if total is not None:
        df['percentage'] = (df[x] / total * 100).round(1)
        text = df['percentage'].astype(str) + '%'
    else:
        text = df[x]
    
    fig = px.bar(
        df, x=x, y=y,
        orientation='h',
        title=title,
        color=x,
        color_continuous_scale='Blues',
        text=text
    )
    fig.update_layout(
        yaxis={'categoryorder':'total ascending'},
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=20, r=20, t=60, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2a3f5f'),
        coloraxis_showscale=False
    )
    fig.update_traces(textposition='outside')
    st.plotly_chart(fig, use_container_width=True)

def plot_upload_heatmap_from_daily(data):
    upload_times = []
    for _, df in data.get("daily", []):
        if 'Upload_Date' in df.columns:
            df = df.copy()
            df['Upload_Date'] = pd.to_datetime(df['Upload_Date'], errors='coerce')
            upload_times.extend(df['Upload_Date'].dropna().tolist())

    if not upload_times:
        return None

    df_all = pd.DataFrame({'Upload_Date': upload_times})
    df_all['hour'] = df_all['Upload_Date'].dt.hour
    df_all['weekday'] = df_all['Upload_Date'].dt.weekday

    heatmap_data = pd.DataFrame(0, index=range(7), columns=range(24))
    grouped = df_all.groupby(['weekday', 'hour']).size()
    for (w, h), c in grouped.items():
        heatmap_data.loc[w, h] = c

    fig = px.imshow(
        heatmap_data,
        labels=dict(x="Hour", y="Weekday", color="Count"),
        x=[f"{h}:00" for h in range(24)],
        y=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
        color_continuous_scale='Blues'
    )
    fig.update_layout(
        title="Video Upload Time Distribution (from daily_videos)",
        xaxis_nticks=24,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2a3f5f')
    )
    return fig


def get_video_history(bvid, data, removed_info):
    history = []
    for day, df in data.get("active_videos", []):
        if 'bvid' not in df.columns:
            continue
        matched = df[df['bvid'] == bvid].copy()
        if matched.empty:
            continue

        combined_row = matched.select_dtypes(include='number').sum().to_dict()
        latest_meta = matched.iloc[-1].to_dict()

        record = {
            'Date': pd.to_datetime(day),
            'bvid': bvid,
            'Title': latest_meta.get('Title', ''),
            'Keyword': latest_meta.get('Keyword', '')
        }
        record.update(combined_row)
        history.append(record)

    result = pd.DataFrame(history)
    if 'Date' in result.columns:
        result = result.sort_values("Date")
        result['Status'] = 'Active'
        if not removed_info.empty and 'bvid' in removed_info.columns:
            if bvid in removed_info['bvid'].values:
                remove_date = pd.to_datetime(
                    removed_info.loc[removed_info['bvid'] == bvid, 'removed_date'].values[0],
                    errors='coerce'
                )
                result.loc[result['Date'] >= remove_date, 'Status'] = 'Removed'
    else:
        return pd.DataFrame()

    return result

def analyze_removed_videos(data, removed_info):
    removed_bvs = set()
    for _, videos in data["removed"]:
        for v in videos:
            if isinstance(v, dict):
                bv = v.get('BV') or v.get('bvid') or v.get('bv')
                if bv: removed_bvs.add(bv)
    
    lifecycle_data = []
    for bv in removed_bvs:
        video_info = removed_info[removed_info['bvid'] == bv].iloc[0] if not removed_info.empty and 'bvid' in removed_info.columns else None
        
        history = []
        for day, df in data["daily"]:
            video_data = df[df['bvid'] == bv] if 'bvid' in df.columns else None
            if video_data is not None and not video_data.empty:
                history.append({
                    "Date": pd.to_datetime(day),
                    "Views": video_data.iloc[0]['Views'] if 'Views' in video_data.columns else 0,
                    "Likes": video_data.iloc[0]['Likes'] if 'Likes' in video_data.columns else 0
                })
        
        if history and video_info is not None:
            first_day = history[0]['Date']
            last_day = history[-1]['Date']
            lifespan = (last_day - first_day).days
            max_views = max(h['Views'] for h in history)
            max_likes = max(h['Likes'] for h in history)
            
            lifecycle_data.append({
                "BV": bv,
                "Keyword": video_info.get('Keyword', 'Unknown'),
                "Title": video_info.get('Title', 'Unknown'),
                "First_Date": first_day,
                "Last_Date": last_day,
                "Lifespan": lifespan,
                "Max_Views": max_views,
                "Max_Likes": max_likes
            })
    
    return pd.DataFrame(lifecycle_data)

def get_sorted_keywords_from_df(df):
    return sorted(
        df['Keyword']
        .dropna()
        .astype(str)
        .loc[lambda x: x.str.strip() != '']
        .unique()
    )

st.set_page_config(
    page_title="Bilibili Tracker Dashboard", 
    layout="wide",
    page_icon="📊"
)

st.title("📊 Bilibili Video Tracker Dashboard")

# Sidebar
st.sidebar.header("🔍 Filters")
min_date = datetime(2025, 4, 17)
max_date = datetime.now(timezone(timedelta(hours=8))).date()
start_date = st.sidebar.date_input("Start Date", max_date - timedelta(days=7), min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

if start_date > end_date:
    st.sidebar.error("End date must be after start date.")

show_daily_summary = st.sidebar.checkbox("Show Daily Summaries", value=False)

chart_options = st.sidebar.multiselect(
    "Display Sections",
    ["New Videos", "Removed Videos", "Engagement", "Tracking Pool", "Upload Heatmap", "Video/Keyword Analysis"],
    default=["New Videos", "Removed Videos", "Engagement", "Tracking Pool", "Upload Heatmap", "Video/Keyword Analysis"]
)

prediction_days = st.sidebar.selectbox(
    "Prediction Days", [0, 1, 3, 7], 
    format_func=lambda x: f"{x} days" if x > 0 else "No prediction"
)

data = load_snapshots(start_date, end_date)
removed_info = load_removed_videos_info()

if show_daily_summary and "daily_summaries" in data and len(data["daily_summaries"]) > 0:
    st.header("📝 Daily Summaries")
    for summary in data["daily_summaries"]:
        with st.expander(f"📅 {summary['date']}", expanded=False):
            st.text(summary['content'])
    st.markdown("---")

st.header("📊 Overview")
cols = st.columns(4)


# 1. Total New Videos
if "New Videos" in chart_options and "new_videos" in data and len(data["new_videos"]) > 0:
    new_count = len(data["new_videos"][-1][1])
    prev_new = len(data["new_videos"][0][1]) if len(data["new_videos"]) > 1 else new_count
    change_pct = (new_count - prev_new) / prev_new * 100 if prev_new != 0 else 0
    cols[0].metric("New Videos", new_count, f"{change_pct:.1f}%")

# 2. Removed Videos
if "Removed Videos" in chart_options and "removed" in data and len(data["removed"]) > 0:
    removed_count = len(data["removed"][-1][1])
    prev_removed = len(data["removed"][0][1]) if len(data["removed"]) > 1 else removed_count
    change_pct = (removed_count - prev_removed) / prev_removed * 100 if prev_removed != 0 else 0
    cols[1].metric("Removed Videos", removed_count, f"{change_pct:.1f}%")

# 3. Tracking Pool Size
if "Tracking Pool" in chart_options and "daily" in data and len(data["daily"]) > 0:
    tracked_count = len(data["daily"][-1][1])
    prev_count = len(data["daily"][0][1]) if len(data["daily"]) > 1 else tracked_count
    change_pct = (tracked_count - prev_count) / prev_count * 100 if prev_count != 0 else 0
    cols[2].metric("Tracking Pool", tracked_count, f"{change_pct:.1f}%")

# 4. Active Videos 
if "active_videos" in data and len(data["active_videos"]) > 0:
    active_count = len(data["active_videos"][-1][1])  
    prev_active = len(data["active_videos"][0][1]) if len(data["active_videos"]) > 1 else active_count
    change_pct = (active_count - prev_active) / prev_active * 100 if prev_active != 0 else 0
    cols[3].metric("Active Videos", active_count, f"{change_pct:+.1f}%")
else:
    cols[3].metric("Active Videos", "N/A", "Insufficient data")

# --- Main Content
if not data:
    st.warning("No data available for selected period.")
else:
    # 1. Upload Heatmap
    if "Upload Heatmap" in chart_options and "daily" in data:
        st.header("🌡️ Upload Time Heatmap")
        fig = plot_upload_heatmap_from_daily(data)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No upload data available for heatmap.")
        st.markdown("---")


    # 2. New Videos Analysis
    if "New Videos" in chart_options and "new_videos" in data:
        st.header("🆕 New Videos Analysis")
        
        daily_new = {day: len(videos) for day, videos in data["new_videos"]}
        new_df = pd.DataFrame({
            "Date": pd.to_datetime(list(daily_new.keys())),
            "Count": list(daily_new.values())
        }).sort_values("Date")
        
        show_growth_metrics(new_df, "Count")
        
        fig = plot_lineplot(new_df, "Date", "Count",
                          "Daily New Videos Trend",
                          predict_days=prediction_days)
        st.plotly_chart(fig, use_container_width=True)
        
        keyword_counter = {}
        for _, videos in data["new_videos"]:
            for v in videos:
                if isinstance(v, dict):
                    kw = v.get("Keyword", "Unknown")
                    keyword_counter[kw] = keyword_counter.get(kw, 0) + 1
        
        if keyword_counter:
            keyword_df = pd.DataFrame({
                "Keyword": list(keyword_counter.keys()),
                "Count": list(keyword_counter.values())
            }).sort_values("Count", ascending=False).head(10)
            
            plot_horizontal_bar(keyword_df, "Count", "Keyword",
                              "Top Keywords for New Videos",
                              total=sum(keyword_counter.values()))
        
        st.markdown("---")

    # 3. Removed Videos Analysis
    if "Removed Videos" in chart_options and "removed" in data and len(data["removed"]) > 0:
        st.header("🗑️ Removed Videos Analysis")
        
        daily_removed = {day: len(videos) for day, videos in data["removed"]}
        removed_df = pd.DataFrame({
            "Date": pd.to_datetime(list(daily_removed.keys())),
            "Count": list(daily_removed.values())
        }).sort_values("Date")
        
        if not removed_df.empty:
            if "new_videos" in data and "daily" in data:
                new_count = sum(len(v) for _, v in data["new_videos"])
                initial_count = len(data["daily"][0][1]) if len(data["daily"]) > 0 else 0
                total_videos = new_count + initial_count
                mortality_rate = removed_df['Count'].sum() / total_videos * 100 if total_videos > 0 else 0
                st.subheader(f"Removal Mortality Rate: {mortality_rate:.1f}%")
            
            show_growth_metrics(removed_df, "Count")
            
            fig = plot_lineplot(removed_df, "Date", "Count",
                              "Daily Removed Videos Trend",
                              predict_days=prediction_days)
            st.plotly_chart(fig, use_container_width=True)
            
            if not removed_info.empty:
                lifecycle_df = analyze_removed_videos(data, removed_info)
                if not lifecycle_df.empty:
                    st.subheader("Removed Videos Lifecycle Analysis")
                    
                    if 'Keyword' in lifecycle_df.columns:
                        kw_lifecycle = lifecycle_df.groupby('Keyword').agg({
                            'Lifespan': 'mean',
                            'Max_Views': 'mean',
                            'Count': 'size'
                        }).reset_index().rename(columns={'Count': 'Video_Count'})
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            fig = plot_lineplot(
                                lifecycle_df.groupby('First_Date').size().reset_index().rename(columns={0: 'Count'}),
                                "First_Date", "Count",
                                "Removed Videos by First Appearance Date",
                                predict_days=prediction_days
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        with col2:
                            plot_horizontal_bar(
                                kw_lifecycle.sort_values('Lifespan', ascending=False).head(10),
                                "Lifespan", "Keyword",
                                "Average Lifespan by Keyword (Days)"
                            )
                    
                    st.dataframe(lifecycle_df.sort_values('Lifespan', ascending=False))
        
        st.markdown("---")

    # 4. Engagement Analysis
    if "Engagement" in chart_options and "engagement" in data:
        st.header("❤️ Engagement Metrics")

        eng_df = data["engagement"]  

        for metric in ["Views", "Likes", "Danmaku", "Coins", "Favorites", "Shares", "Comments"]:
            st.subheader(metric)
            show_growth_metrics(eng_df, metric)
            fig = plot_lineplot(eng_df, "Date", metric,
                                f"Daily {metric} Trend",
                                predict_days=prediction_days)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")


    # 5. Tracking Pool Analysis
    if "Tracking Pool" in chart_options and "daily" in data and len(data["daily"]) > 0:
        st.header("📊 Tracking Pool Analysis")
        
        pool_df = pd.DataFrame([
            {"Date": pd.to_datetime(day), "Count": len(df)}
            for day, df in data["daily"]
        ])
        
        show_growth_metrics(pool_df, "Count")
        
        fig = plot_lineplot(pool_df, "Date", "Count",
                          "Pool Size Trend",
                          predict_days=prediction_days)
        st.plotly_chart(fig, use_container_width=True)
        
        latest_df = data["daily"][-1][1]
        
        if 'Keyword' in latest_df.columns:
            keyword_dist = latest_df['Keyword'].value_counts().reset_index()
            keyword_dist.columns = ["Keyword", "Count"]
            keyword_dist = keyword_dist.head(10)
            
            plot_horizontal_bar(keyword_dist, "Count", "Keyword",
                              "Top Keywords in Tracking Pool",
                              total=len(latest_df))
            
            st.subheader("Keyword Analysis")
            selected_keyword = st.selectbox(
                "Select a Keyword",
                sorted(latest_df['Keyword'].unique().tolist())
            )
            keyword_count = len(latest_df[latest_df['Keyword'] == selected_keyword])
            total_count = len(latest_df)
            percentage = (keyword_count / total_count) * 100 if total_count > 0 else 0
            
            st.metric(
                f"Videos with Keyword: {selected_keyword}",
                f"{keyword_count} ({percentage:.1f}%)"
            )
        
        display_cols = []
        for col in ['BV', 'bvid', 'bv']:
            if col in latest_df.columns:
                display_cols.append(col)
                break
        if 'Title' in latest_df.columns:
            display_cols.append('Title')
        for col in ['Views', 'Likes', 'Danmaku', 'Coins', 'Shares', 'Favorites', 'Comments']:
            if col in latest_df.columns:
                display_cols.append(col)
        
        if len(display_cols) >= 3:
            top20_videos = latest_df.sort_values('Likes', ascending=False).head(20)
            st.subheader("Current Top 20 Videos in Pool")
            st.dataframe(top20_videos[display_cols])
        
        st.markdown("---")

    # 6. Video/Keyword Analysis
    if "Video/Keyword Analysis" in chart_options:
        st.header("🔍 Video/Keyword Detailed Analysis (from Tracking Pool)")

        tracking_pool_path = TRACKING_POOL_FILE

        if os.path.exists(tracking_pool_path):
            try:
                with open(tracking_pool_path, "r", encoding="utf-8") as f:
                    raw_pool_data = json.load(f)
                    tracking_pool = raw_pool_data.get("videos", raw_pool_data)

                pool_data = []
                for bvid, video_data in tracking_pool.items():
                    if "fields" in video_data:
                        fields = video_data["fields"]
                        other = {k: v for k, v in video_data.items() if k != "fields"}
                        combined = {**fields, **other}
                    else:
                        combined = video_data

                    combined["bvid"] = bvid
                    pool_data.append(combined)

                pool_df = pd.DataFrame(pool_data)

                tab1, tab2 = st.tabs(["Single Video Analysis", "Keyword Analysis"])

                with tab1:
                    st.subheader("Single Video Analysis")

                    video_options = []
                    bvid_map = {}

                    for _, row in pool_df.iterrows():
                        bvid = row.get('bvid', '')
                        if not bvid:
                            continue
                        title = str(row.get('Title', row.get('title', 'No Title'))).strip()
                        keyword = str(row.get('Keyword', row.get('keyword', ''))).strip() or 'No Keyword'
                        upload_date = str(row.get('Upload_Date', row.get('upload_date', '')))
                        label = f"{bvid} | {keyword} | {title[:40]} | {upload_date}"
                        video_options.append(label)
                        bvid_map[label] = bvid

                    if not video_options:
                        st.warning("No videos found in tracking pool.")
                    else:
                        selected_label = st.selectbox("Select a Video", video_options, key="video_select")
                        selected_video = bvid_map[selected_label]
                        video_details = pool_df[pool_df['bvid'] == selected_video].iloc[0].to_dict()

                        info_cols = st.columns(3)
                        with info_cols[0]:
                            st.metric("BVID", selected_video)
                            if 'Link' in video_details:
                                st.markdown(f"[🔗 Original Link]({video_details['Link']})")

                        with info_cols[1]:
                            st.metric("Upload Date", video_details.get('Upload_Date', 'N/A'))
                            st.metric("Keyword", video_details.get('Keyword', 'N/A'))

                        with info_cols[2]:
                            st.metric("Duration", video_details.get('Duration', 'N/A'))
                            st.metric("Uploader", video_details.get('Uploader', 'N/A'))

                        video_history = get_video_history(selected_video, data, removed_info)

                        if isinstance(video_history, pd.DataFrame) and not video_history.empty:
                            active_history = video_history[video_history['Status'] == 'Active']

                            metrics_to_show = ["Views", "Likes", "Danmaku", "Coins", "Favorites", "Shares", "Comments"]
                            available_metrics = [m for m in metrics_to_show if m in active_history.columns]

                            if available_metrics:
                                cols = st.columns(2)
                                for i, metric in enumerate(available_metrics):
                                    with cols[i % 2]:
                                        fig = go.Figure()
                                        fig.add_trace(go.Scatter(
                                            x=active_history['Date'],
                                            y=active_history[metric],
                                            mode='lines+markers',
                                            name='Actual',
                                            line=dict(width=2)
                                        ))

                                        if prediction_days > 0 and len(active_history) >= 3:
                                            df = active_history[['Date', metric]].dropna().reset_index(drop=True)
                                            if len(df) >= 3:
                                                df['Day_Num'] = range(len(df))
                                                X = df[['Day_Num']]
                                                y_vals = df[metric]

                                                model = LinearRegression()
                                                model.fit(X, y_vals)

                                                future_X = pd.DataFrame({'Day_Num': range(len(df), len(df) + prediction_days)})
                                                future_dates = [df['Date'].iloc[-1] + timedelta(days=i + 1) for i in range(prediction_days)]
                                                future_y = model.predict(future_X)

                                                full_pred_dates = [df['Date'].iloc[-1]] + future_dates
                                                full_pred_y = [df[metric].iloc[-1]] + list(future_y)

                                                fig.add_trace(go.Scatter(
                                                    x=full_pred_dates,
                                                    y=full_pred_y,
                                                    mode='lines+markers',
                                                    name=f"Prediction +{prediction_days}d",
                                                    line=dict(dash='dot', color='#ff7f0e')
                                                ))

                                        fig.update_layout(
                                            title=f"{metric} Trend",
                                            xaxis_title="Date",
                                            yaxis_title=metric,
                                            hovermode="x unified",
                                            margin=dict(l=20, r=20, t=60, b=20),
                                            plot_bgcolor='rgba(0,0,0,0)',
                                            paper_bgcolor='rgba(0,0,0,0)',
                                            font=dict(color='#2a3f5f')
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("No engagement metrics available for this video.")
                        else:
                            st.warning("No historical data available for selected video.")

                # Tab 2
                with tab2:
                    st.subheader("Keyword Analysis")

                    if 'Keyword' in pool_df.columns:
                        keyword_dist = (
                            pool_df['Keyword']
                            .dropna()
                            .astype(str)
                            .loc[lambda x: x.str.strip() != '']
                            .value_counts()
                            .reset_index()
                        )
                        keyword_dist.columns = ["Keyword", "Count"]

                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total Keywords", len(keyword_dist))
                        with col2:
                            st.metric("Total Videos in Pool", len(pool_df))

                        plot_horizontal_bar(
                            keyword_dist.head(20),
                            "Count", "Keyword",
                            "Top 20 Keywords in Tracking Pool"
                        )

                        available_keywords = sorted(pool_df['Keyword'].dropna().unique())
                        if available_keywords:
                            selected_keyword = st.selectbox(
                                "Select a Keyword for Detailed Analysis",
                                available_keywords,
                                key="keyword_select"
                            )

                            keyword_videos = pool_df[pool_df['Keyword'] == selected_keyword]

                            st.subheader(f"Statistics for Keyword: {selected_keyword}")
                            if not keyword_videos.empty:
                                avg_values = {}
                                for metric in ["Views", "Likes", "Danmaku", "Coins", "Favorites", "Shares", "Comments"]:
                                    if metric in keyword_videos.columns:
                                        avg_values[f"Avg {metric}"] = keyword_videos[metric].mean()

                                cols = st.columns(4)
                                for i, (name, value) in enumerate(avg_values.items()):
                                    cols[i % 4].metric(name, f"{value:,.1f}")

                                display_columns = ['bvid', 'Title', 'Upload_Date', 'Views', 'Likes',
                                                'Danmaku', 'Coins', 'Favorites', 'Shares', 'Comments']
                                display_columns = [col for col in display_columns if col in keyword_videos.columns]

                                format_dict = {}
                                for col in display_columns:
                                    if pd.api.types.is_numeric_dtype(keyword_videos[col]):
                                        format_dict[col] = "{:,.0f}"

                                st.dataframe(
                                    keyword_videos[display_columns]
                                    .sort_values('Views', ascending=False)
                                    .style.format(format_dict)
                                )
                            else:
                                st.warning(f"No videos found for keyword: {selected_keyword}")
                        else:
                            st.warning("No keywords available in tracking pool.")
                    else:
                        st.warning("Keyword column missing in tracking pool data.")
            except Exception as e:
                st.error(f"Error loading tracking pool data: {str(e)}")
        else:
            st.warning(f"Tracking pool file not found at: {tracking_pool_path}")
