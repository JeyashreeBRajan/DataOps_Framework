import pandas as pd
import json

def transform_weather(input_file="weather_raw.json", output_file="weather_clean.json"):
    """
    Cleans and standardizes weather data before loading.
    Works on multiple records saved in weather_raw.json
    """

    records = []
    with open(input_file, "r") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"❌ Skipping invalid JSON line: {line}")

    if not records:
        print("⚠️ No records found for transformation")
        return []

    df = pd.DataFrame(records)

    # Round numeric values
    df["temperature"] = df["temperature"].round(1)
    df["wind_speed"] = df["wind_speed"].round(1)

    # Standardize casing
    df["city"] = df["city"].str.title()
    df["weather"] = df["weather"].str.title()

    # Ensure timestamp is proper datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Save cleaned data
    df.to_json(output_file, orient="records", lines=True, date_format="iso")

    print(f"✅ Transformed {len(df)} records → saved to {output_file}")

    return df.to_dict(orient="records")
