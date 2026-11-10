import numpy as np
import pandas as pd
import pytest

from src.transformation.data_processor import AISDataProcessor, haversine_nm

T0 = pd.Timestamp("2023-01-01 00:00", tz="UTC")


def ping(mmsi, minutes, lat, lon, sog, status=0, name="TEST", **extra):
    return {"MMSI": mmsi, "BaseDateTime": T0 + pd.Timedelta(minutes=minutes), "LAT": lat,
            "LON": lon, "SOG": sog, "COG": 0.0, "Heading": 0.0, "Status": status,
            "VesselName": name, "VesselType": 70, **extra}


def test_haversine_one_degree_of_latitude_is_60_nm():
    assert haversine_nm(0.0, 0.0, 1.0, 0.0) == pytest.approx(60.04, abs=0.05)


def test_clean_data_drops_invalid_rows_and_nulls_sentinels():
    raw = pd.DataFrame([
        ping(219018671, 0, 35.76, -5.83, 12.0),                  # valid
        ping(219018671, 0, 35.76, -5.83, 12.0),                  # duplicate
        ping(123, 10, 35.76, -5.83, 12.0),                       # invalid MMSI
        ping(219018671, 20, 95.0, -5.83, 12.0),                  # invalid latitude
        ping(219018671, 30, 0.0, 0.0, 12.0),                     # "null island"
        ping(219018671, 40, 35.77, -5.82, 80.0),                 # impossible speed
        ping(219018671, 50, 35.78, -5.81, 11.0, Heading=511.0),  # heading not available
    ])
    clean = AISDataProcessor().clean_data(raw)

    assert len(clean) == 2
    assert clean["MMSI"].dtype == np.int64
    assert clean["Heading"].isna().sum() == 1


def test_metrics_moving_docked_and_gap():
    p = AISDataProcessor()
    df = pd.DataFrame([
        # Vessel A: steams north at ~12 kn for two hours (0.2 deg lat ~ 12 nm per hour)
        ping(111111111, 0, 35.0, -5.0, 12.0),
        ping(111111111, 60, 35.2, -5.0, 12.0),
        ping(111111111, 120, 35.4, -5.0, 12.0),
        # Vessel B: moored for two hours
        ping(222222222, 0, 35.9, -5.5, 0.0, status=5),
        ping(222222222, 60, 35.9, -5.5, 0.0, status=5),
        ping(222222222, 120, 35.9, -5.5, 0.0, status=5),
        # Vessel C: anchored (status 1), then a 5h coverage gap
        ping(333333333, 0, 36.0, -6.0, 0.0, status=1),
        ping(333333333, 300, 36.0, -6.0, 0.0, status=1),
    ])
    m = p.calculate_vessel_metrics(p.clean_data(df)).set_index("mmsi")

    a, b, c = m.loc[111111111], m.loc[222222222], m.loc[333333333]
    assert a.total_distance_nm == pytest.approx(24.0, rel=0.01)
    assert (a.moving_time_hours, a.at_dock_time_hours) == (2.0, 0.0)
    assert (b.moving_time_hours, b.at_dock_time_hours) == (0.0, 2.0)
    assert (c.total_time_hours, c.moving_time_hours, c.at_dock_time_hours) == (5.0, 0.0, 0.0)
    assert (m.moving_time_hours + m.at_dock_time_hours <= m.total_time_hours + 1e-9).all()


def test_gps_glitch_is_excluded_from_distance():
    p = AISDataProcessor()
    df = pd.DataFrame([
        ping(111111111, 0, 35.0, -5.0, 10.0),
        ping(111111111, 10, 45.0, -5.0, 10.0),   # 600 nm in 10 minutes: glitch
    ])
    m = p.calculate_vessel_metrics(p.clean_data(df))
    assert m.loc[0, "total_distance_nm"] == 0


def test_distance_across_coverage_gap_is_not_counted():
    p = AISDataProcessor()
    df = pd.DataFrame([
        ping(111111111, 0, 35.0, -5.0, 10.0),
        ping(111111111, 10, 35.03, -5.0, 10.0),            # ~1.8 nm in 10 min: counted
        ping(111111111, 60 * 24 * 30, 40.0, -5.0, 10.0),   # reappears a month later: not counted
    ])
    m = p.calculate_vessel_metrics(p.clean_data(df))
    assert m.loc[0, "total_distance_nm"] == pytest.approx(1.8, abs=0.05)


def test_metrics_on_empty_frame():
    assert AISDataProcessor().calculate_vessel_metrics(pd.DataFrame()).empty
