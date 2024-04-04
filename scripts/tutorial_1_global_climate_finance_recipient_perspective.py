from climate_finance import ClimateData, set_climate_finance_data_path
import pandas as pd
from scripts import config

set_climate_finance_data_path(config.Paths.raw_data)


def load_data_into_dataframe(
    climate_data: ClimateData, methodology: str, flows: str, source: str
) -> pd.DataFrame:

    climate_data.load_spending_data(methodology=METHODOLOGY, flows=FLOWS, source=SOURCE)

    data = climate_data.get_data()

    return data


def aggregate_by_year(df: pd.DataFrame) -> pd.DataFrame:
    groupby = ["year"]
    columns_to_keep = ["year", "value"]

    return (
        df.groupby(by=groupby, observed=True, as_index=False)
        .sum("value")
        .filter(items=columns_to_keep, axis=1)
    )


if __name__ == "__main__":

    # The best way to specify the climate finance data you need is to instantiate the
    # ClimateData class. Here, you can specify the years, providers, recipients,
    # currency, prices, and base_year if relevant.
    climate_data = ClimateData(
        years=range(2013, 2022),
        providers=None,  # This takes flows from all providers
        recipients=None,  # This takes flows to all recipients
        currency="USD",
        prices="current",
        #base_year=2022,
    )

    # You can specify the methodology here. Either ONE or OECD.
    METHODOLOGY = "ONE"

    # You can specify the flow type here. The main ones are gross_disbursement and
    # commitments. Use ClimateData.available_flows() to see other available flows.
    FLOWS = "gross_disbursements"

    # depending on the methodology and flows, you need to use different sources. This is
    # explained in significant detail in the package documentation. Here, we focus on the
    # data reported in the CRDF and ONE's analysis. For:
    # - OECD Commitments: 'OECD_CRDF'
    # - OECD Disbursements: 'OECD_CRDF_CRS_ALLOCABLE'
    # - ONE Commitments: 'OECD_CRDF'
    # - ONE Disbursments: 'OECD_CRDF_CRS_ALLOCABLE'

    SOURCE = "OECD_CRDF_CRS_ALLOCABLE"

    full_dataset = load_data_into_dataframe(
        climate_data=climate_data, methodology=METHODOLOGY, flows=FLOWS, source=SOURCE
    )

    global_climate_finance_annual_total = aggregate_by_year(full_dataset)
    print(global_climate_finance_annual_total['value'].sum()/1000000000)
