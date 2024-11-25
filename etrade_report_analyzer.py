import argparse
import json
import locale

import pandas as pd
import requests
from prettytable import PrettyTable

CURRENCY_API = "https://api.frankfurter.app"
EXCHANGE_CACHE = []
LATEST_TIMESTAMP = None

locale.setlocale(locale.LC_ALL, "de_DE.UTF-8")


def get_exchange_trade(
    date: str = "latest", base: str = "USD", return_currency: str = None
):
    full_url = CURRENCY_API + "/" + date + f"?base={base}"
    rates = None
    if len(EXCHANGE_CACHE) >= 1:
        for dict_elem in EXCHANGE_CACHE:
            if dict_elem.get("date") == date:
                rates = dict_elem.get("rates")
    if rates is None:
        req = requests.get(full_url)
        if req.status_code == 200:
            api_return = json.loads(req.content.decode("utf-8"))
            rates = api_return.get("rates")
            assert isinstance(rates, dict), f"Returned rates are not dict: {rates}"
            EXCHANGE_CACHE.append(api_return)
    if return_currency:
        return rates.get(return_currency)
    return rates


def format_value(value):
    if isinstance(value, float):
        return locale.currency(value, symbol=False, grouping=True)
    else:
        return str(value)


def read_csv(input_file: str) -> list[dict]:
    return_list: list = []
    df = pd.read_csv(input_file, delimiter=";", decimal=",", skip_blank_lines=True)

    # Select relevant columns
    columns_of_interest = [
        "Plan Type",
        "Order Type",
        "Qty.",
        "Date Sold",
        "Order Number",
        "Adjusted Gain/Loss Per Share",
        "Adjusted Cost Basis Per Share",
        "Purchase Price",
    ]
    df = df[columns_of_interest]

    # Rename columns for easier access
    df.columns = [
        "PlanType",
        "OrderType",
        "SharesSold",
        "SaleDate",
        "OrderNumber",
        "GainLossPerShare",
        "AdjustedCostPerShare",
        "PurchasePrice",
    ]

    # Strip dollar signs, commas, and ensure numeric conversion
    for col in ["GainLossPerShare", "AdjustedCostPerShare"]:
        df[col] = (
            df[col]
            .astype(str)  # Ensure the column is treated as a string
            .str.replace(",", ".", regex=False)  # Replace comma as decimal separator
            .replace("[\$,]", "", regex=True)  # Remove dollar signs
            .replace("nan", "0")  # Handle NaN values explicitly
            .astype(float)  # Convert back to float
        )

    # Handle missing values (if any)
    df["AdjustedCostPerShare"] = df["AdjustedCostPerShare"].fillna(0.0)

    # Convert "Date Sold" to datetime format and extract the year
    df["SaleDate"] = pd.to_datetime(df["SaleDate"], format="%m/%d/%Y", errors="coerce")
    df["Year"] = df["SaleDate"].dt.year

    for _, row in df.iterrows():
        return_list.append(
            {
                "OrderNumber": str(row.get("OrderNumber")),
                "SaleDate": row["SaleDate"],
                "OrderType": row.get("OrderType"),
                "PlanType": row.get("PlanType"),
                "SharesSold": row.get("SharesSold"),
                "AdjustedCostPerShare": row.get("AdjustedCostPerShare", 0.0),
                "GainLossPerShare": row.get("GainLossPerShare", 0.0),
                "PurchasePrice": row.get("PurchasePrice", 0.0),
                "is_selltocover": True if "STC" in str(row["OrderType"]) else False,
            }
        )

    return return_list


def generate_pretty_table_with_hierarchy(
    data,
    filter_year=None,
    exclude_fields=["PurchasePrice"],
    exclude_total=[
        "Gewinn/Verlust",
        "Order",
        "Verkaufsdatum",
        "Rabatt (inkl.)",
        "Type",
    ],
):
    # Ensure the input is a list of dictionaries
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise TypeError("Input data must be a list of dictionaries.")

    # Use an empty list as default for exclude_fields
    default_exclude = ["is_selltocover"]
    exclude_fields = exclude_fields + default_exclude or default_exclude

    # Parsing input structure and grouping by Year and PlanType
    grouped_data = {}

    for i, entry in enumerate(data):  # Track the index of the dictionary
        try:
            year = entry["Verkaufsdatum"].year
            if filter_year:
                if year != int(filter_year):
                    continue
            entry["Verkaufsdatum"] = entry["Verkaufsdatum"].strftime("%d.%m.%Y")
            plan_type = entry["PlanType"]
        except KeyError as e:
            raise KeyError(f"Missing key '{e.args[0]}' in entry at index {i}: {entry}")

        if year not in grouped_data:
            grouped_data[year] = {}

        if plan_type not in grouped_data[year]:
            grouped_data[year][plan_type] = []

        grouped_data[year][plan_type].append(entry)

    # Dynamically create tables for each Year and PlanType
    tables = []
    for year, plans in grouped_data.items():
        for plan_type, records in plans.items():
            # Create a PrettyTable
            table = PrettyTable()
            # Make sure that "Year" and "PlanType" are unique columns
            # and do not conflict with existing keys
            existing_keys = list(records[0].keys())

            # Exclude the specified fields from the table columns
            field_names = ["Year", "PlanType"] + [
                key
                for key in existing_keys
                if key not in ["Year", "PlanType", *exclude_fields]
            ]

            table.field_names = field_names

            # Add rows for the current PlanType, including Year and PlanType
            for record in records:
                row = [year, plan_type] + [
                    format_value(record.get(field, "")) for field in field_names[2:]
                ]
                table.add_row(row)

            table.add_row(["----" for field in field_names])
            # Add the "Total" row for numeric fields
            total_row = [
                "Total",
                "",
            ]  # First two fields are Year and PlanType, we leave them as "Total" and empty
            for field in field_names[2:]:
                # If the field is numeric, sum the values
                column_values = [record.get(field, 0) for record in records]
                if field not in exclude_total:  # Check if all values are numeric
                    total_row.append(
                        format_value(
                            sum(
                                [
                                    x if isinstance(x, (int, float)) else 0
                                    for x in column_values
                                ]
                            )
                        )
                    )
                else:
                    total_row.append(
                        ""
                    )  # Non-numeric fields get an empty value in the total row

            table.add_row(total_row)

            tables.append(table)

    return tables


def parse_data(data: list[dict], exchange: bool = False):
    parsed_data = []
    if len(data) < 1:
        return parsed_data

    for entry in data:
        exchange_rate = None
        espp = True if entry.get("PlanType") == "ESPP" else False
        is_selltocover = entry.get("is_selltocover", False)
        purchase_price_per_share = (
            entry.get("AdjustedCostPerShare", 0.0)
            if not espp
            else entry.get("PurchasePrice", 0.0)
        )
        sell_price_per_share = (
            (entry.get("AdjustedCostPerShare") + entry.get("GainLossPerShare", 0.0))
            if not espp
            else entry.get("AdjustedCostPerShare", 0.0)
        )
        gainloss_per_share = (
            entry.get("GainLossPerShare", 0.0)
            if not espp
            else (sell_price_per_share - purchase_price_per_share)
        )
        total_value = sell_price_per_share * entry.get("SharesSold", 0)
        kapitalertrag_usd = gainloss_per_share * entry.get("SharesSold", 0)
        if exchange:
            exchange_rate = get_exchange_trade(
                date=str(entry.get("SaleDate").strftime("%Y-%m-%d")),
                return_currency="EUR",
            )
        kapitalertrag_eur = "n/a"
        if exchange_rate:
            kapitalertrag_eur = kapitalertrag_usd * exchange_rate
        updated_dict = {
            "Order": str(entry.get("OrderNumber")),
            "Verkaufsdatum": entry["SaleDate"],
            "Type": entry.get("OrderType"),
            "PlanType": entry.get("PlanType"),
            "Anz.": entry.get("SharesSold"),
            "Kaufwert": purchase_price_per_share,
            "Gewinn/Verlust": gainloss_per_share,
            "Verkaufswert": sell_price_per_share,
            "Orderwert": total_value,
            "KapitalErtrag (USD)": kapitalertrag_usd,
            "KapitalErtrag (EUR)": kapitalertrag_eur,
            "is_selltocover": is_selltocover,
        }
        if espp:
            updated_dict.update({"Rabatt (inkl.)": "15%"})
        parsed_data.append(updated_dict)
    return parsed_data


def main():
    parser = argparse.ArgumentParser(description="Process broker depot reports.")
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to the input CSV file containing the broker report.",
    )
    parser.add_argument(
        "--include-sell-to-cover",
        action="store_true",
        default=False,
        dest="selltocover",
        help="Include 'Sale to Cover' orders in the report (default: exclude).",
    )
    parser.add_argument(
        "--year",
        type=int,
        dest="year",
        help="Filter data by year (default: no filter)",
    )
    parser.add_argument(
        "--include-exchange",
        action="store_true",
        default=False,
        dest="include_exchange",
        help="Add USD->EUR for KapitalErtrag",
    )

    args = parser.parse_args()

    data = parse_data(
        read_csv(input_file=args.input_file), exchange=args.include_exchange
    )
    new_data = []
    if len(data) > 0:
        for elem in data:
            if elem.get("is_selltocover", False) and not args.selltocover:
                continue
            new_data.append(elem)
    tables = generate_pretty_table_with_hierarchy(new_data, filter_year=args.year)
    if len(tables) > 0:
        for table in tables:
            print(table)


if __name__ == "__main__":
    main()
