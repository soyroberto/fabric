# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8d1be20-f67d-4f99-991f-96e677b5a6f7",
# META       "default_lakehouse_name": "Spotify2_Analytics_Lakehouse",
# META       "default_lakehouse_workspace_id": "d91f59ec-1250-4694-ba8a-944d3b9f3d55",
# META       "known_lakehouses": [
# META         {
# META           "id": "d8d1be20-f67d-4f99-991f-96e677b5a6f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------------------------------
# 1. Full list of ISO-3166 Alpha-2 country codes
# ---------------------------------------------------------------------
iso_countries = {
    "AF": "Afghanistan", "AL": "Albania", "DZ": "Algeria", "AS": "American Samoa",
    "AD": "Andorra", "AO": "Angola", "AI": "Anguilla", "AG": "Antigua and Barbuda",
    "AR": "Argentina", "AM": "Armenia", "AW": "Aruba", "AU": "Australia",
    "AT": "Austria", "AZ": "Azerbaijan", "BS": "Bahamas", "BH": "Bahrain",
    "BD": "Bangladesh", "BB": "Barbados", "BY": "Belarus", "BE": "Belgium",
    "BZ": "Belize", "BJ": "Benin", "BM": "Bermuda", "BT": "Bhutan",
    "BO": "Bolivia", "BA": "Bosnia and Herzegovina", "BW": "Botswana", "BR": "Brazil",
    "IO": "British Indian Ocean Territory", "BN": "Brunei", "BG": "Bulgaria",
    "BF": "Burkina Faso", "BI": "Burundi", "KH": "Cambodia", "CM": "Cameroon",
    "CA": "Canada", "CV": "Cape Verde", "KY": "Cayman Islands",
    "CF": "Central African Republic", "TD": "Chad", "CL": "Chile", "CN": "China",
    "CO": "Colombia", "KM": "Comoros", "CG": "Congo", "CD": "DR Congo",
    "CR": "Costa Rica", "HR": "Croatia", "CU": "Cuba", "CY": "Cyprus",
    "CZ": "Czech Republic", "DK": "Denmark", "DJ": "Djibouti", "DM": "Dominica",
    "DO": "Dominican Republic", "EC": "Ecuador", "EG": "Egypt",
    "SV": "El Salvador", "GQ": "Equatorial Guinea", "ER": "Eritrea",
    "EE": "Estonia", "ET": "Ethiopia", "FJ": "Fiji", "FI": "Finland",
    "FR": "France", "GA": "Gabon", "GM": "Gambia", "GE": "Georgia",
    "DE": "Germany", "GH": "Ghana", "GR": "Greece", "GL": "Greenland",
    "GD": "Grenada", "GU": "Guam", "GT": "Guatemala", "GN": "Guinea",
    "GW": "Guinea-Bissau", "GY": "Guyana", "HT": "Haiti", "HN": "Honduras",
    "HK": "Hong Kong", "HU": "Hungary", "IS": "Iceland", "IN": "India",
    "ID": "Indonesia", "IR": "Iran", "IQ": "Iraq", "IE": "Ireland",
    "IL": "Israel", "IT": "Italy", "CI": "Ivory Coast", "JM": "Jamaica",
    "JP": "Japan", "JO": "Jordan", "KZ": "Kazakhstan", "KE": "Kenya",
    "KI": "Kiribati", "KP": "North Korea", "KR": "South Korea", "KW": "Kuwait",
    "KG": "Kyrgyzstan", "LA": "Laos", "LV": "Latvia", "LB": "Lebanon",
    "LS": "Lesotho", "LR": "Liberia", "LY": "Libya", "LI": "Liechtenstein",
    "LT": "Lithuania", "LU": "Luxembourg", "MO": "Macao", "MG": "Madagascar",
    "MW": "Malawi", "MY": "Malaysia", "MV": "Maldives", "ML": "Mali",
    "MT": "Malta", "MH": "Marshall Islands", "MQ": "Martinique",
    "MR": "Mauritania", "MU": "Mauritius", "MX": "Mexico",
    "MD": "Moldova", "MC": "Monaco", "MN": "Mongolia", "ME": "Montenegro",
    "MA": "Morocco", "MZ": "Mozambique", "MM": "Myanmar", "NA": "Namibia",
    "NR": "Nauru", "NP": "Nepal", "NL": "Netherlands", "NZ": "New Zealand",
    "NI": "Nicaragua", "NE": "Niger", "NG": "Nigeria", "NO": "Norway",
    "OM": "Oman", "PK": "Pakistan", "PW": "Palau", "PA": "Panama",
    "PG": "Papua New Guinea", "PY": "Paraguay", "PE": "Peru",
    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "PR": "Puerto Rico",
    "QA": "Qatar", "RO": "Romania", "RU": "Russia", "RW": "Rwanda",
    "KN": "Saint Kitts and Nevis", "LC": "Saint Lucia",
    "VC": "Saint Vincent and the Grenadines", "WS": "Samoa",
    "SM": "San Marino", "ST": "Sao Tome and Principe", "SA": "Saudi Arabia",
    "SN": "Senegal", "RS": "Serbia", "SC": "Seychelles", "SL": "Sierra Leone",
    "SG": "Singapore", "SK": "Slovakia", "SI": "Slovenia",
    "SB": "Solomon Islands", "SO": "Somalia", "ZA": "South Africa",
    "ES": "Spain", "LK": "Sri Lanka", "SD": "Sudan", "SR": "Suriname",
    "SE": "Sweden", "CH": "Switzerland", "SY": "Syria", "TW": "Taiwan",
    "TJ": "Tajikistan", "TZ": "Tanzania", "TH": "Thailand", "TL": "Timor-Leste",
    "TG": "Togo", "TO": "Tonga", "TT": "Trinidad and Tobago",
    "TN": "Tunisia", "TR": "Turkey", "TM": "Turkmenistan",
    "TV": "Tuvalu", "UG": "Uganda", "UA": "Ukraine",
    "AE": "United Arab Emirates", "GB": "United Kingdom",
    "US": "United States", "UY": "Uruguay", "UZ": "Uzbekistan",
    "VU": "Vanuatu", "VE": "Venezuela", "VN": "Vietnam",
    "YE": "Yemen", "ZM": "Zambia", "ZW": "Zimbabwe"
}

# ---------------------------------------------------------------------
# 2. Convert dictionary to Spark DataFrame
# ---------------------------------------------------------------------
country_df = spark.createDataFrame(
    [(k, v) for k, v in iso_countries.items()],
    schema=StructType([
        StructField("country", StringType(), True),
        StructField("country_name", StringType(), True)
    ])
)

# ---------------------------------------------------------------------
# 3. Load main Spotify table
# ---------------------------------------------------------------------
df = spark.read.table("universal_top_spotify_songs")

# ---------------------------------------------------------------------
# 4. Join to enrich with country name
# ---------------------------------------------------------------------
enriched = (
    df
    .withColumn("country", when(col("country").isin("", None), None).otherwise(col("country")))
    .join(country_df, on="country", how="left")
    .withColumn("country_name",
        when(col("country").isNull(), "Global")
        .when(col("country_name").isNull(), "Other")
        .otherwise(col("country_name"))
    )
    .withColumnRenamed("snapshot_date", "chart_date")
)

# ---------------------------------------------------------------------
# 5. Save for Power BI
# ---------------------------------------------------------------------
enriched.write.format("delta").mode("overwrite").saveAsTable("spotify_for_powerbi")

print("✅ Ready for Power BI! Use table 'spotify_for_powerbi'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
