from climate_finance import ClimateData, set_climate_finance_data_path
from scripts import config

set_climate_finance_data_path(config.Paths.raw_data)

climate_data = ClimateData(
    years=range(2018, 2022),
    providers=4,
    recipients=755,
    currency='USD',
    prices='current',
    #base_year=2022
    )

# Set your custom methodology
climate_data.set_custom_spending_methodology(
    coefficients=(0.5, 0.85),
    highest_marker=True
)

# Download the data
climate_data.load_spending_data(
    methodology='custom',
    source='OECD_CRDF_CRS_ALLOCABLE',
    flows='gross_disbursements'
)

# Load data into a Pandas DataFrame
df = climate_data.get_data()