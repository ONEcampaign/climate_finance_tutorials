import pandas as pd
from climate_finance import ClimateData, set_climate_finance_data_path
from scripts import config

from climate_finance.methodologies.bilateral.tools import rio_markers_multi_codes, rio_markers_bilat_codes, rio_markers_all_codes()

set_climate_finance_data_path(config.Paths.raw_data)

def get_provider_list(reporting_methodology: str, provider_type: str) -> list[str]:
    """
    Args:
        - methodology: the reporting methodology of the required providers (rio or
        climate_components).
        - providers: the type of the required providers (bilateral or multilateral)

    Returns: List of donor_codes of the specified donors.
    """

    if reporting_methodology == "rio_markers":
        if provider_type == "bilateral":
            providers == rio_markers_bilat_codes()
        if provider_type == "multilateral":
            providers == rio_markers_multi_codes()
        else: print("invalid methodology and provider_type input")
    if reporting_methodology == "climate_components"
        if provider_type == "bilateral":
            providers == rio_markers_bilat_codes()
        if provider_type == "multilateral":
            providers == rio_markers_multi_codes()
        else: print("invalid methodology and provider_type input")

    rio_markers_multi_codes = rio_markers_multi_codes()

    rio_markers_bilat_codes = rio_markers_bilat_codes()

    rio_markers_all_codes = rio_markers_all_codes()

    return rio_markers_bilat_codes

def instantiate_climatedata_class_by_provider_list(
    years: list[int] | range, recipients: list[str], currency: str, prices: str, base_year: int
) -> ClimateData:
    """
    This function creates an instance of the ClimateData class depending on the
    specified provider function (which sets a list of providers)
    Providers need to be seperated between Rio Marker and Climate Component providers.
    Depending on the methodology, we need to use a different source of data (i.e. for
    Rio Marker providers, we use the CRS, and for climate components, we need to use
    the CRDF for commitments, and match the CRDF to the CRS for disbursements).

    Returns: an instance of the ClimateData Class for bilateral Rio Marker providers.
    """

    providers = get_provider_list()

    climate_data = ClimateData(
        years=years,
        providers=providers,  # This takes flows from all providers
        recipients=recipients,  # This takes flows to all recipients
        currency=currency,
        prices=prices,
        base_year=base_year,
    )

    return climate_data

def load_bilateral_rio_marker_providers(climate_data:ClimateData) -> pd.DataFrame


if __name__ == "__main__":

    YEARS = range(2021, 2023)
    RECIPIENTS = None
    CURRENCY = "USD"
    PRICES = "constant"
    BASE_YEAR = 2021

    METHODOLOGY = "OECD"
    FLOWS = "commitments"
    SOURCE = "OECD_CRDF"

    bilateral_rio_markers = instantiate_climatedata_class_by_provider_list(
        years=YEARS,
        recipients=RECIPIENTS,
        currency=CURRENCY,
        prices=PRICES,
        base_year=BASE_YEAR,
    )
