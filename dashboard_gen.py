
import os, textwrap, pandas as pd
import matplotlib.pyplot as plt

def parse_dates_safe(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def chunk_iter(path, parse_date_cols=None, usecols=None, chunksize=250_000):
    opts = dict(low_memory=False, chunksize=chunksize)
    if usecols is not None:
        opts["usecols"] = usecols
    for chunk in pd.read_csv(path, **opts):
        if parse_date_cols:
            chunk = parse_dates_safe(chunk, parse_date_cols)
        yield chunk

def finalize_plot(save_path, title, xlabel=None, ylabel=None):
    plt.title(title)
    if xlabel: plt.xlabel(xlabel)
    if ylabel: plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()

def users_dashboard(users_path, out_dir):
    weekly_counts = []
    for chunk in chunk_iter(users_path, parse_date_cols=["signup_date"], usecols=["signup_date"]):
        s = chunk["signup_date"].dropna().dt.to_period("W").value_counts().sort_index()
        weekly_counts.append(s)
    if weekly_counts:
        weekly = pd.concat(weekly_counts, axis=0).groupby(level=0).sum().sort_index()
        weekly = weekly.rename_axis("week").reset_index(name="signups")
        plt.figure()
        plt.plot(weekly["week"].astype(str), weekly["signups"])
        plt.xticks(rotation=45, ha="right")
        finalize_plot(os.path.join(out_dir, "users_signups_weekly.png"), "Weekly Signups", "Week", "Signups")

    tier_counts = []
    for chunk in chunk_iter(users_path, usecols=["plan_tier"]):
        tier_counts.append(chunk["plan_tier"].value_counts())
    if tier_counts:
        vc = pd.concat(tier_counts, axis=0).groupby(level=0).sum().sort_values(ascending=False)
        top = vc.head(15)
        plt.figure()
        top.plot(kind="bar")
        finalize_plot(os.path.join(out_dir, "users_plan_tier.png"), "Users by Plan Tier (Top 15)", "Plan Tier", "Users")

    acq_counts = []
    for chunk in chunk_iter(users_path, usecols=["acquisition_channel"]):
        acq_counts.append(chunk["acquisition_channel"].value_counts())
    if acq_counts:
        vc = pd.concat(acq_counts, axis=0).groupby(level=0).sum().sort_values(ascending=False)
        top = vc.head(15)
        plt.figure()
        top.plot(kind="bar")
        finalize_plot(os.path.join(out_dir, "users_acquisition_channel.png"), "Acquisition Channels (Top 15)", "Channel", "Users")

def sessions_dashboard(sessions_path, out_dir):
    daily_counts = []
    for chunk in chunk_iter(sessions_path, parse_date_cols=["session_start"], usecols=["session_start"]):
        s = chunk["session_start"].dropna().dt.to_period("D").value_counts().sort_index()
        daily_counts.append(s)
    if daily_counts:
        daily = pd.concat(daily_counts, axis=0).groupby(level=0).sum().sort_index()
        daily = daily.rename_axis("day").reset_index(name="sessions")
        plt.figure()
        plt.plot(daily["day"].astype(str), daily["sessions"])
        plt.xticks(rotation=45, ha="right")
        finalize_plot(os.path.join(out_dir, "sessions_daily.png"), "Daily Sessions", "Day", "Sessions")

    hist_counts = []
    for chunk in chunk_iter(sessions_path, usecols=["session_length_sec"]):
        hist_counts.append(chunk["session_length_sec"].dropna())
    if hist_counts:
        vals = pd.concat(hist_counts, axis=0)
        cap = vals.quantile(0.99)
        vals = vals[vals <= cap]
        plt.figure()
        plt.hist(vals, bins=50, edgecolor="black")
        finalize_plot(os.path.join(out_dir, "sessions_length_hist.png"), "Session Length Distribution (≤99th percentile)", "Seconds", "Frequency")

    device_counts = []
    for chunk in chunk_iter(sessions_path, usecols=["device"]):
        device_counts.append(chunk["device"].value_counts())
    if device_counts:
        vc = pd.concat(device_counts, axis=0).groupby(level=0).sum().sort_values(ascending=False)
        top = vc.head(10)
        plt.figure()
        top.plot(kind="bar")
        finalize_plot(os.path.join(out_dir, "sessions_device_top10.png"), "Top Devices by Session Count", "Device", "Sessions")

def events_dashboard(events_path, out_dir):
    daily_counts = []
    for chunk in chunk_iter(events_path, parse_date_cols=["ts"], usecols=["ts"]):
        s = chunk["ts"].dropna().dt.to_period("D").value_counts().sort_index()
        daily_counts.append(s)
    if daily_counts:
        daily = pd.concat(daily_counts, axis=0).groupby(level=0).sum().sort_index()
        daily = daily.rename_axis("day").reset_index(name="events")
        plt.figure()
        plt.plot(daily["day"].astype(str), daily["events"])
        plt.xticks(rotation=45, ha="right")
        finalize_plot(os.path.join(out_dir, "events_daily.png"), "Daily Events", "Day", "Events")

    feat_counts = []
    for chunk in chunk_iter(events_path, usecols=["feature_name"]):
        feat_counts.append(chunk["feature_name"].value_counts())
    if feat_counts:
        vc = pd.concat(feat_counts, axis=0).groupby(level=0).sum().sort_values(ascending=False)
        top = vc.head(20)
        plt.figure()
        top.plot(kind="bar")
        finalize_plot(os.path.join(out_dir, "events_feature_top20.png"), "Top 20 Features by Event Count", "Feature", "Events")

    lat_parts = []
    for chunk in chunk_iter(events_path, usecols=["latency_ms"]):
        lat_parts.append(chunk["latency_ms"].dropna())
    if lat_parts:
        vals = pd.concat(lat_parts, axis=0)
        cap = vals.quantile(0.99)
        vals = vals[vals <= cap]
        plt.figure()
        plt.hist(vals, bins=60, edgecolor="black")
        finalize_plot(os.path.join(out_dir, "events_latency_hist.png"), "Event Latency (ms, ≤99th percentile)", "Latency (ms)", "Frequency")

    success_daily = []
    for chunk in chunk_iter(events_path, parse_date_cols=["ts"], usecols=["ts","success"]):
        c = chunk.dropna(subset=["ts"])
        if "success" in c.columns:
            grp = c.groupby(c["ts"].dt.to_period("D"))["success"].mean()
            success_daily.append(grp)
    if success_daily:
        daily = pd.concat(success_daily, axis=0).groupby(level=0).mean().sort_index()
        df = daily.rename_axis("day").reset_index(name="success_rate")
        plt.figure()
        plt.plot(df["day"].astype(str), df["success_rate"])
        plt.xticks(rotation=45, ha="right")
        finalize_plot(os.path.join(out_dir, "events_success_rate_daily.png"), "Daily Success Rate", "Day", "Success Rate")

def billing_dashboard(billing_path, out_dir):
    mrr_monthly = []
    for chunk in chunk_iter(billing_path, parse_date_cols=["month"], usecols=["month","mrr"]):
        c = chunk.dropna(subset=["month"])
        grp = c.groupby(c["month"].dt.to_period("M"))["mrr"].sum()
        mrr_monthly.append(grp)
    if mrr_monthly:
        monthly = pd.concat(mrr_monthly, axis=0).groupby(level=0).sum().sort_index()
        df = monthly.rename_axis("month").reset_index(name="mrr_total")
        plt.figure()
        plt.plot(df["month"].astype(str), df["mrr_total"])
        plt.xticks(rotation=45, ha="right")
        finalize_plot(os.path.join(out_dir, "billing_mrr_monthly.png"), "Total MRR by Month", "Month", "MRR")

    mrr_by_tier = {}
    for chunk in chunk_iter(billing_path, usecols=["plan_tier","mrr"]):
        grp = chunk.groupby("plan_tier")["mrr"].sum()
        for k, v in grp.items():
            mrr_by_tier[k] = mrr_by_tier.get(k, 0) + v
    if mrr_by_tier:
        s = pd.Series(mrr_by_tier).sort_values(ascending=False).head(8)
        plt.figure()
        s.plot(kind="bar")
        finalize_plot(os.path.join(out_dir, "billing_mrr_by_tier.png"), "MRR by Plan Tier (Top 8)", "Plan Tier", "MRR")

    points = []
    for chunk in chunk_iter(billing_path, usecols=["active_seats","mrr"]):
        c = chunk.dropna(subset=["active_seats","mrr"]).sample(n=min(5000, len(chunk)), random_state=42)
        points.append(c)
    if points:
        df = pd.concat(points, axis=0)
        plt.figure()
        plt.scatter(df["active_seats"], df["mrr"], s=5)
        finalize_plot(os.path.join(out_dir, "billing_seats_vs_mrr.png"), "Active Seats vs. MRR (sampled)", "Active Seats", "MRR")

def run_all(base_dir="/mnt/data", out_dir="/mnt/data/dashboard"):
    os.makedirs(out_dir, exist_ok=True)
    paths = {
        "users": os.path.join(base_dir, "users.csv"),
        "sessions": os.path.join(base_dir, "sessions.csv"),
        "events": os.path.join(base_dir, "events.csv"),
        "billing": os.path.join(base_dir, "billing.csv"),
    }
    users_dashboard(paths["users"], out_dir)
    sessions_dashboard(paths["sessions"], out_dir)
    events_dashboard(paths["events"], out_dir)
    billing_dashboard(paths["billing"], out_dir)

if __name__ == "__main__":
    run_all(base_dir=".", out_dir="./dashboard")
    print("Data printed in ./dashboard")
