"""Cleaning and per-vessel metrics for AIS positions.

Everything is vectorised with pandas/NumPy (grouped shift() instead of Python loops
over rows with .iloc and per-pair geodesic calls).
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

EARTH_RADIUS_NM = 3440.065


def haversine_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in nautical miles (works on scalars or NumPy arrays)."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_NM * np.arcsin(np.sqrt(a))


class AISDataProcessor:
    MAX_SOG_KN = 50.0          # realistic upper bound for merchant vessels
    MOVING_SPEED_KN = 1.0      # a segment is "moving" if its average SOG is >= 1 kn
    MOORED_STATUS = 5          # AIS navigational status "Moored"
    MAX_GAP_HOURS = 2.0        # longer gaps = vessel out of coverage, time not attributed
    MAX_IMPLIED_SPEED_KN = 60  # faster jumps between pings are GPS glitches

    # ------------------------------------------------------------------ cleaning
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Type coercion, validation and de-duplication of raw AIS rows."""
        initial = len(df)
        df = df.copy()

        df["BaseDateTime"] = pd.to_datetime(df["BaseDateTime"], errors="coerce", utc=True)
        for col in ("MMSI", "LAT", "LON", "SOG", "COG", "Heading", "Status", "VesselType"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["BaseDateTime", "LAT", "LON", "MMSI"])
        df = df[df["MMSI"].between(100_000_000, 999_999_999)]          # valid 9-digit MMSI
        df = df[df["LAT"].between(-90, 90) & df["LON"].between(-180, 180)]
        df = df[~((df["LAT"] == 0) & (df["LON"] == 0))]                  # "null island" fixes
        df = df[df["SOG"].between(0, self.MAX_SOG_KN)]                   # also drops missing SOG

        # AIS "not available" sentinels -> NULL instead of failing DB constraints
        if "Heading" in df.columns:
            df.loc[~df["Heading"].between(0, 360), "Heading"] = np.nan   # 511 = not available
        if "COG" in df.columns:
            df.loc[~df["COG"].between(0, 360), "COG"] = np.nan

        df["MMSI"] = df["MMSI"].astype("int64")
        df = df.drop_duplicates(subset=["MMSI", "BaseDateTime"])
        df = df.sort_values(["MMSI", "BaseDateTime"]).reset_index(drop=True)

        logger.info("Cleaning: %s -> %s rows (%s dropped)", initial, len(df), initial - len(df))
        return df

    # ------------------------------------------------------------------ metrics
    def calculate_vessel_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """One row per vessel: distance, moving/at-dock time, speeds, first/last seen."""
        columns = [
            "mmsi", "vessel_name", "vessel_type", "total_distance_nm", "total_time_hours",
            "moving_time_hours", "at_dock_time_hours", "point_count", "avg_speed_knots",
            "max_speed_knots", "first_seen", "last_seen",
        ]
        if df.empty:
            return pd.DataFrame(columns=columns)

        df = df.sort_values(["MMSI", "BaseDateTime"]).reset_index(drop=True)
        g = df.groupby("MMSI", sort=False)

        # Segment = consecutive pair of pings of the same vessel
        prev_lat = g["LAT"].shift()
        prev_lon = g["LON"].shift()
        prev_time = g["BaseDateTime"].shift()
        prev_sog = g["SOG"].shift()

        seg_nm = haversine_nm(prev_lat, prev_lon, df["LAT"], df["LON"])
        seg_h = (df["BaseDateTime"] - prev_time).dt.total_seconds() / 3600
        implied_kn = seg_nm / seg_h.where(seg_h > 0)
        glitch = implied_kn > self.MAX_IMPLIED_SPEED_KN

        in_coverage = seg_h.notna() & (seg_h <= self.MAX_GAP_HOURS)
        moving = in_coverage & (((df["SOG"] + prev_sog) / 2) >= self.MOVING_SPEED_KN)
        if "Status" in df.columns and df["Status"].notna().any():
            moored = g["Status"].shift() == self.MOORED_STATUS
        else:
            moored = pd.Series(True, index=df.index)   # no status data: stationary = at dock
        at_dock = in_coverage & ~moving & moored

        # Distance is only counted while the vessel is in coverage: a straight line across a
        # multi-hour (or multi-month) gap says nothing about the route actually sailed.
        seg = pd.DataFrame({
            "MMSI": df["MMSI"],
            "dist": seg_nm.where(in_coverage & ~glitch, 0).fillna(0),
            "moving_h": seg_h.where(moving, 0).fillna(0),
            "dock_h": seg_h.where(at_dock, 0).fillna(0),
        })
        sums = seg.groupby("MMSI").sum()

        out = pd.DataFrame({
            "vessel_name": g["VesselName"].first() if "VesselName" in df else None,
            "vessel_type": g["VesselType"].first() if "VesselType" in df else None,
            "total_distance_nm": sums["dist"],
            "total_time_hours": (g["BaseDateTime"].max() - g["BaseDateTime"].min()).dt.total_seconds() / 3600,
            "moving_time_hours": sums["moving_h"],
            "at_dock_time_hours": sums["dock_h"],
            "point_count": g.size(),
            "avg_speed_knots": g["SOG"].mean(),
            "max_speed_knots": g["SOG"].max(),
            "first_seen": g["BaseDateTime"].min(),
            "last_seen": g["BaseDateTime"].max(),
        })
        out = out.reset_index().rename(columns={"MMSI": "mmsi"})
        logger.info("Metrics computed for %s vessels", len(out))
        return out[columns]
