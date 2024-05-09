import pandas as pd
from climate_finance import ClimateData, set_climate_finance_data_path
from scripts import config

set_climate_finance_data_path(config.Paths.raw_data)


def get_cff_bulk_download_custom(climate_data: ClimateData) -> pd.DataFrame:

    climate_data.set_custom_spending_methodology(
        coefficients=(1, 1), highest_marker=False
    )

    climate_data.load_spending_data(
        methodology="custom",
        source="OECD_CRDF_CRS_ALLOCABLE",
        flows="gross_disbursements",
    )

    return climate_data.get_data()


def get_cff_bulk_download_oecd(climate_data: ClimateData) -> pd.DataFrame:

    climate_data.load_spending_data(
        methodology="OECD", source="OECD_CRS_ALLOCABLE", flows="gross_disbursements"
    )

    return climate_data.get_data()


def _green_wall_accelerator_pattern() -> str:
    substrings = ["green wall accelerator", "Great Green Wall", "PAAGGW", "GGW"]

    return "|".join(substrings)


def green_wall_accelerator_projects(data: pd.DataFrame) -> pd.DataFrame:
    pattern = _green_wall_accelerator_pattern()

    mask = data["project_title"].str.contains(
        pattern, na=False, case=False, regex=True
    ) | data["description"].str.contains(pattern, na=False, case=False, regex=True)

    df = data[mask].reset_index(drop=True)

    return df


def ggwa_pipeline(climate_data: ClimateData, methodology: str) -> pd.DataFrame:

    if methodology == "custom":
        data = get_cff_bulk_download_custom(climate_data=climate_data)
    elif methodology == "oecd":
        data = get_cff_bulk_download_oecd(climate_data=climate_data)
    else:
        print("incorrect methodology input")

    ggwa_data = green_wall_accelerator_projects(data)

    return ggwa_data


def pivot_to_stop_double_counting(df: pd.DataFrame) -> pd.DataFrame:
    data = df.pivot_table(
        index=[
            "year",
            "oecd_provider_code",
            "provider",
            "agency_name",
            "oecd_agency_code",
            "oecd_recipient_code",
            "recipient",
            "flow_code",
            "flow_name",
            "sector_code",
            "sector_name",
            "purpose_code",
            "purpose_name",
            "project_title",
            "crs_id",
            "project_id",
            "description",
            "type_of_finance",
            "level",
            "modality",
            "flow_type",
            "source",
            "currency",
            "prices",
        ],
        columns="indicator",  # The new columns will be named after unique values in 'indicator'
        values="value",  # Fill values in the new columns with the corresponding 'value'
        aggfunc="sum",  # You can change the aggregation function to 'mean', 'max', etc., if needed
    )

    # Reset index to turn the multi-level index into columns if necessary
    return data.reset_index()


if __name__ == "__main__":
    dac_donors = [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        18,
        20,
        21,
        22,
        40,
        50,
        61,
        68,
        69,
        75,
        76,
        84,
        301,
        302,
        701,
        742,
        801,
        820,
        918,
    ]
    multilaterals = [
        104,
        807,
        811,
        812,
        901,
        902,
        903,
        905,
        906,
        907,
        909,
        910,
        913,
        914,
        915,
        921,
        923,
        926,
        928,
        932,
        940,
        944,
        948,
        951,
        952,
        953,
        954,
        956,
        958,
        959,
        960,
        962,
        963,
        964,
        966,
        967,
        971,
        974,
        976,
        978,
        979,
        980,
        981,
        982,
        983,
        988,
        990,
        992,
        997,
        1011,
        1012,
        1013,
        1014,
        1015,
        1016,
        1017,
        1018,
        1019,
        1020,
        1023,
        1024,
        1025,
        1037,
        1038,
        1039,
        1041,
        1044,
        1045,
        1046,
        1047,
        1048,
        1049,
        1050,
        1052,
        1311,
        1312,
        1313,
        1401,
        1406,
    ]

    dac_donors_and_multilaterals = dac_donors + multilaterals

    climate_data = ClimateData(
        years=range(2020, 2022 + 1),
        providers=dac_donors_and_multilaterals,
        recipients=None,
        currency="USD",
        prices="current",
        # base_year=None
    )

    # custom_data = ggwa_pipeline(climate_data=climate_data, methodology='custom')
    oecd_data = ggwa_pipeline(climate_data=climate_data, methodology="oecd").pipe(
        pivot_to_stop_double_counting
    )
    custom_data = ggwa_pipeline(climate_data=climate_data, methodology="oecd")

    # custom_data.to_csv(config.Paths.output / "custom_output_2020_2022.csv", index=False)
    #oecd_data.to_csv(config.Paths.output / "oecd_output_2020_2022_new.csv", index=False)
