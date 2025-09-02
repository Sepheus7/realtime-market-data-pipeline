import os
import time
import shutil
import tempfile
from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Realtime Market Pipeline", layout="wide")

db_path = os.getenv("DUCKDB_PATH", os.path.join("data", "pipeline.duckdb"))
st.sidebar.write("DB:", db_path)
refresh_sec = st.sidebar.slider("Refresh seconds", 1, 10, 2)
window_min = st.sidebar.slider("Window minutes", 1, 30, 5)
symbols = st.sidebar.text_input("Symbols filter (comma-separated, blank=all)", "")

def get_conn(path: str):
    # Open read-only to avoid writer lock conflicts
    return duckdb.connect(path, read_only=True)

def load_data(con, minutes: int, symbols_csv: str) -> pd.DataFrame:
    base_sql = f"""
        select symbol, window_end as ts, log_return, volatility, num_ticks, last_price, latency_ms
        from features
        where window_end > now() - INTERVAL {minutes} MINUTE
        order by ts
    """
    df = con.execute(base_sql).df()
    if symbols_csv.strip():
        keep = [s.strip().upper() for s in symbols_csv.split(',') if s.strip()]
        df = df[df['symbol'].isin(keep)]
    return df

placeholder = st.empty()
snapshot_dir = Path(tempfile.gettempdir()) / "rtm_snapshots"
snapshot_dir.mkdir(parents=True, exist_ok=True)

while True:
    try:
        # Create a read-only snapshot copy to avoid locking the live DB
        src = Path(db_path)
        snap = snapshot_dir / f"pipeline_snapshot_{int(time.time()*1000)}.duckdb"
        try:
            if src.exists():
                shutil.copy2(src, snap)
        except Exception as copy_err:
            # If copy fails, fall back to reading the live DB (read-only)
            snap = src

        con = get_conn(str(snap))
        df = load_data(con, window_min, symbols)
        total_rows = 0
        try:
            total_rows = con.execute("select count(*) from features").fetchone()[0]
        except Exception:
            total_rows = len(df)
        # KPIs
        avg_latency = float(df["latency_ms"].mean()) if "latency_ms" in df.columns and not df.empty else 0.0
        with placeholder.container():
            k1, k2, k3 = st.columns(3)
            k1.metric("Rows in window", f"{len(df):,}")
            k2.metric("Total rows", f"{int(total_rows):,}")
            k3.metric("Avg latency (ms)", f"{avg_latency:.0f}")
            if df.empty:
                st.info("No data yet. Ensure the consumer is running with --sink duckdb.")
            else:
                # Pivot to wide format: index=timestamp, columns=symbol
                returns_wide = (
                    df.pivot_table(index='ts', columns='symbol', values='log_return', aggfunc='last')
                    .sort_index()
                )
                vol_wide = (
                    df.pivot_table(index='ts', columns='symbol', values='volatility', aggfunc='last')
                    .sort_index()
                )
                price_wide = (
                    df.pivot_table(index='ts', columns='symbol', values='last_price', aggfunc='last')
                    .sort_index()
                )
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Windowed log return (per symbol)")
                    st.line_chart(returns_wide)
                with col2:
                    st.subheader("Windowed volatility proxy (per symbol)")
                    st.line_chart(vol_wide)
                st.subheader("Price (last price at window end)")
                if not price_wide.empty:
                    syms = list(price_wide.columns)
                    tabs = st.tabs([f"{s}" for s in syms])
                    for tab, s in zip(tabs, syms):
                        with tab:
                            st.line_chart(price_wide[[s]].rename(columns={s: "price"}))
        try:
            con.close()
        except Exception:
            pass

        # Cleanup old snapshots to avoid clutter (keep last 5)
        try:
            snaps = sorted(snapshot_dir.glob("pipeline_snapshot_*.duckdb"))
            for old in snaps[:-5]:
                old.unlink(missing_ok=True)
        except Exception:
            pass
        time.sleep(refresh_sec)
    except Exception as e:
        st.error(str(e))
        time.sleep(refresh_sec)


