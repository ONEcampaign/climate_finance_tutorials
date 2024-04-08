import pandas as pd


def filter_for_climate_related_flows(df: pd.DataFrame) -> pd.DataFrame:

    climate_related = [
        "Mitigation",
        "Adaptation",
        "Cross-cutting",
        "climate_adaptation",
        "climate_mitigation",
        "climate_cross_cutting",
    ]

    return df.loc[lambda d: d.indicator.isin(climate_related)]


def filter_for_dac_donors(df: pd.DataFrame) -> pd.DataFrame:

    dac_donors = [
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "18",
        "20",
        "21",
        "22",
        "40",
        "50",
        "61",
        "68",
        "69",
        "75",
        "76",
        "84",
        "301",
        "302",
        "701",
        "742",
        "801",
        "820",
        "918",
    ]

    return df.loc[lambda d: d.oecd_provider_code.isin(dac_donors)]


def aggregate_by_year(df: pd.DataFrame) -> pd.DataFrame:
    groupby = ["year"]
    columns_to_keep = ["year", "value"]

    return (
        df.groupby(by=groupby, observed=True, as_index=False)
        .sum("value")
        .filter(items=columns_to_keep, axis=1)
    )


def aggregate_by_year_flow_type_and_recipient(df: pd.DataFrame) -> pd.DataFrame:
    groupby = ["year", "oecd_recipient_code", "flow_code", "flow_name"]
    columns_to_keep = ["year", "oecd_recipient_code", "flow_code", "flow_name", "value"]

    return (
        df.groupby(by=groupby, observed=True, as_index=False)
        .sum("value")
        .filter(items=columns_to_keep, axis=1)
    )
