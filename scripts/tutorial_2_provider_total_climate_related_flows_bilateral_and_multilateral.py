from climate_finance import ClimateData, set_climate_finance_data_path
import pandas as pd
from scripts import config

set_climate_finance_data_path(config.Paths.raw_data)


def one_methodology_bilateral_flows_by_recipient(
    climate_data: ClimateData, flows: str
) -> pd.DataFrame:

    climate_data.load_spending_data(
        methodology="ONE", source="OECD_CRS_ALLOCABLE", flows=flows
    )

    data = climate_data.get_data()

    return data


if __name__ == "__main__":

    flows = "gross_disbursements"

    climate_data = ClimateData(
        years=range(
            2021, 2023
        ),  # remember that the range is exclusive of the last year.
        providers=[4],
        recipients=None,  # This takes flows to all recipients
        currency="USD",
        prices="constant",
        base_year=2022,
    )

    climate_flows_by_recipient_one_methodology = one_methodology_by_recipient(
        climate_data=climate_data, source=source, flows=flows
    )
