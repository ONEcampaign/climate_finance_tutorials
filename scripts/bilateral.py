from climate_finance import ClimateData, set_climate_finance_data_path

from climate_finance.methodologies.bilateral.tools import rio_markers_bilat_codes

rio_markers_bilat_codes = rio_markers_bilat_codes()


def instantiate_climatedata_class_for_bilateral_rio_providers(
    years: range(int), currency: str, prices: str, base_year: int
) -> ClimateData:
    """
    This function creates an instance of the ClimateData class using the list of Rio
    Marker providers.
    Providers need to be seperated between Rio Marker and Climate Component providers.
    Depending on the methodology, we need to use a different source of data (i.e. for
    Rio Marker providers, we use the CRS, and for climate components, we need to use
    the CRDF for commitments, and match the CRDF to the CRS for disbursements).

    Returns: an instance of the ClimateData Class for bilateral Rio Marker providers.
    """

    rio_markers_bilat_codes = rio_markers_bilat_codes()

    climate_data = ClimateData(
        years=years,
        providers=rio_markers_bilat_codes,  # This takes flows from all providers
        recipients=None,  # This takes flows to all recipients
        currency=currency,
        prices=prices,
        base_year=2022,
    )

    return climate_data
