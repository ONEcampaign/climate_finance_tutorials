from climate_finance import ClimateData, set_climate_finance_data_path
import pandas as pd
from scripts import config
from scripts.common import filter_for_climate_related_flows

set_climate_finance_data_path(config.Paths.raw_data)


def load_bilateral_data_into_dataframe(
    climate_data: ClimateData, methodology: str, flows: str, source: str
) -> pd.DataFrame:

    climate_data.load_spending_data(
        methodology=METHODOLOGY, flows=FLOWS, source=BILATERAL_SOURCE
    )

    data = climate_data.get_data()

    return data


def load_multilateral_data_into_dataframe(
    climate_data: ClimateData, spending_methodology: str, flows: str, source: str
) -> pd.DataFrame:

    climate_data.load_multilateral_imputations_data(
        spending_methodology=METHODOLOGY,
        flows=FLOWS,
        source=MULTILATERAL_SOURCE,
        rolling_years_spending=1,
        groupby=[
            "year",
            "oecd_provider_code",
            "provider",
            "indicator",
            "flow_type",
            "currency",
            "prices",
        ],
        shareby=[
            "year",
            "oecd_provider_code",
            "provider",
            "flow_type",
            "currency",
            "prices",
        ],
    )

    data = climate_data.get_data()

    return data


if __name__ == "__main__":

    # The best way to specify the climate finance data you need is to instantiate the
    # ClimateData class. Here, you can specify the years, providers, recipients,
    # currency, prices, and base_year if relevant.
    bilateral_climate_data = ClimateData(
        years=range(2020, 2022),
        providers=[4],  # This takes flows from all providers
        recipients=None,  # This takes flows to all recipients
        currency="USD",
        prices="current",
        # base_year=2022,
    )

    # You can specify the methodology here. Either ONE or OECD.
    METHODOLOGY = "OECD"

    # You can specify the flow type here. The main ones are gross_disbursement and
    # commitments. Use ClimateData.available_flows() to see other available flows.
    FLOWS = "commitments"

    # depending on the methodology and flows, you need to use different sources. This is
    # explained in significant detail in the package documentation. Here, we focus on the
    # data reported in the CRDF and ONE's analysis. For:
    # - OECD Commitments: 'OECD_CRDF'
    # - OECD Disbursements: 'OECD_CRDF_CRS_ALLOCABLE'
    # - ONE Commitments: 'OECD_CRDF'
    # - ONE Disbursments: 'OECD_CRDF_CRS_ALLOCABLE'

    BILATERAL_SOURCE = "OECD_CRS"
    MULTILATERAL_SOURCE = "OECD_CRDF"

    bilateral_dataset = load_bilateral_data_into_dataframe(
        climate_data=climate_data,
        methodology=METHODOLOGY,
        flows=FLOWS,
        source=BILATERAL_SOURCE,
    ).pipe(filter_for_climate_related_flows)

    imputed_multilateral_dataset = load_multilateral_data_into_dataframe(
        climate_data=climate_data,
        spending_methodology=METHODOLOGY,
        flows=FLOWS,
        source=MULTILATERAL_SOURCE,
    )
