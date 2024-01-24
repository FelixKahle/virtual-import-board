# Written by Felix Kahle, A123234, felix.kahle@worldcourier.de

from typing import Set
import pandas as pd

def is_mawb_dataframe(df: pd.DataFrame) -> bool:
    """
    Checks if the given dataframe is a valid MAWB dataframe.

    Parameters
    ----------
    df:
        The dataframe to check
    
    Returns
    -------
    bool:
        True if the dataframe is a valid MAWB dataframe, False otherwise.
    """

    # All valid MAWB files contain the following columns
    mawb_columns = [
        "Create Date",
        "Create By",
        "Owner",
        "Load #",
        "Status",
        "Ref: MAWB",
        "Ref: Job Number",
        "Carrier Rate Carrier Name",
        "Ref: Flight Arrival",
        "Actual Ship Unit Quantity",
        "Actual Ship Unit Weight",
        "Ship Unit UOM (Actual Weight)",
        "Carrier Rate",
        "Target Ship (Early)",
        "Actual Ship Date",
        "Shipper Name",
        "Shipper Address",
        "Shipper City",
        "Shipper State",
        "Shipper Postal Code",
        "Shipper Country",
        "Target Delivery (Early)",
        "Actual Delivery Date",
        "Consignee Name",
        "Consignee Address",
        "Consignee City",
        "Consignee State",
        "Consignee Postal Code",
        "Consignee Country",
        "ActStat: Act Stat: Set Booking Status",
        "ActStat: Act Stat: Confirm PostFlight",
        "ActStat: Act Stat: Confirm Transfer 1",
        "ActStat: Act Stat: Confirm Transfer 2",
        "ActStat: Act Stat: Confirm Consignment Arr",
    ]

    return set(df.columns) == set(mawb_columns)

def is_shipper_site_dataframe(df: pd.DataFrame) -> bool:
    """
    Checks if the given dataframe is a valid Shipper Site dataframe.

    Parameters
    ----------
    df:
        The dataframe to check

    Returns
    -------
    bool:
        True if the dataframe is a valid Shipper Site dataframe, False otherwise.
    """

    shipper_site_columns = [
        "Create Date",
        "Create By",
        "Owner",
        "BillTo Code",
        "BillTo Name",
        "Load #",
        "Ref: House Waybill Number",
        "Ref: Temperature Range",
        "ActStat: Act Stat: RecoverTM",
        "ActPlan: Act Plan: Qualification Time",
        "Status",
        "Actual Ship Unit Quantity",
        "Actual Ship Unit Weight",
        "Ship Unit UOM (Actual Weight)",
        "Target Ship (Range)",
        "Actual Ship Date",
        "Ref: Shipper Site",
        "Shipper Name",
        "Shipper City",
        "Shipper State",
        "Shipper Country",
        "Target Delivery (Range)",
        "Actual Delivery Date",
        "ActPlan: Act Plan: Delivery Expiration",
        "Consignee Name",
        "Consignee City",
        "Consignee State",
        "Consignee Country",
        "ActStat: Act Stat: Gather Replenishment Details",
        "ActDate: Act Date: Gather Replenishment Details",
    ]

    return set(df.columns) == set(shipper_site_columns)

def load_mawb_dataframe(mawb_file) -> pd.DataFrame:
    """
    Loads the MAWB xls file into a pandas dataframe and returns it.

    Parameters
    ----------
    mawb_file:
        The MAWB file

    Returns
    -------
    pd.DataFrame:
        The MAWB xls file as a pandas dataframe.
    """

    # The MAWB xls file contains a column "Target Delivery (Early)" that contains dates in the format "mm/dd/yyyy hh:mm".
    date_parser = lambda x: pd.to_datetime(x.strip(), format='%m/%d/%Y %H:%M')
    return pd.read_excel(mawb_file, dtype={"Ref: MAWB": str}, parse_dates=["Target Delivery (Early)"], date_format=date_parser)

def load_shipper_site_dataframe(shipper_site_file) -> pd.DataFrame:
    """
    Loads the Shipper Site xls file into a pandas dataframe and returns it.

    Parameters
    ----------
    shipper_site_file:
        The Shipper Site file

    Returns
    -------
    pd.DataFrame:
        The Shipper Site xls file as a pandas dataframe.
    """
    return pd.read_excel(shipper_site_file)

def process_mawb_dataframe(mawb_df: pd.DataFrame, consolidate: bool) -> pd.DataFrame:
    """
    Processes the MAWB dataframe and returns it.
    The resulting dataframe is filtered to only contain the following columns:
    - MAWB
    - Job Number
    - Airline Name
    - Flight Arrival
    - Shipper Airport City
    - Shipper Airport State
    - Shipper Airport Postal Code
    - Target Delivery Airport
    - Consignee Airport Name
    - Consignee Airport City
    - Consignee Airport State
    - Consignee Airport Country

    Rows that do not contain a MAWB number are dropped.
    Rows that contain multiple Ref: Job Numbers are copied.

    Parameters
    ----------
    mawb_df:
        The MAWB dataframe
    
    consolidate:
        If True, rows that contain multiple Ref: Job Numbers are copied. If False, they remain as they are.
    
    Returns
    -------
    pd.DataFrame
        The MAWB xls file as a pandas dataframe.
    """

    # Select the columns that should be loaded into the dataframe
    # The columns are found inside the MAWB .xls file
    select = [
        "Ref: MAWB", 
        "Ref: Job Number", 
        "Carrier Rate Carrier Name",
        "Ref: Flight Arrival", 
        "Shipper City", 
        "Shipper State", 
        "Shipper Postal Code", 
        "Target Delivery (Early)", 
        "Consignee Name", 
        "Consignee City", 
        "Consignee State", 
        "Consignee Country"
    ]
    mawb_df = mawb_df.loc[:, select]

    # Rename columns
    mawb_df.rename(columns={
        "Ref: MAWB": "MAWB", 
        "Ref: Job Number": "Job Number",
        "Carrier Rate Carrier Name": "Airline Name",
        "Ref: Flight Arrival": "Flight Arrival",
        "Shipper City": "Shipper Airport City",
        "Shipper State": "Shipper Airport State",
        "Shipper Postal Code": "Shipper Airport Postal Code",
        "Target Delivery (Early)": "Target Delivery Airport",
        "Consignee Name": "Consignee Airport Name",
        "Consignee City": "Consignee Airport City",
        "Consignee State": "Consignee Airport State",
        "Consignee Country": "Consignee Airport Country"
    }, inplace=True)

    # Drop rows where "Ref: MAWB" is empty or contains no data
    mawb_df.dropna(subset=["MAWB"], inplace=True)

    # Copy all rows where "Ref: Job Number" contains multiple Job Numbers if consolidate is False
    # Print a warning if consolidate is True, as this is not very useful (?)
    if not consolidate:
        mawb_df["Job Number"] = mawb_df["Job Number"].str.split(",").apply(lambda x: [item.strip() for item in x])
        mawb_df = mawb_df.explode("Job Number", ignore_index=True)
    else:
        print("WARNING: Consolidation is enabled. Rows that contain multiple Job Numbers are not copied.")

    # Correct the state columns. Only state codes with a length of 2 are valid.
    # All other values are set to None.
    columns_to_check_state = ["Shipper Airport State", "Consignee Airport State"]
    mawb_df[columns_to_check_state] = mawb_df[columns_to_check_state].map(
        lambda item: item if isinstance(item, str) and len(item) == 2 else None
    )

    # Capitalize the first letter of each word in the "Consignee Airport Name" and "Shipper Airport City" column
    columns_to_title = ["Shipper Airport City", "Consignee Airport Name", "Consignee Airport City", "Airline Name"]
    mawb_df[columns_to_title] = mawb_df[columns_to_title].map(lambda x: x.title() if isinstance(x, str) else x)

    # Compute the desired MAWB format
    # We want the MAWB to consist of the first three letters, a dash, and the last eight letters.
    # Use vectorized string slicing and concatenation to accelerate the process.
    # Check if the MAWB is 11 characters long to prevent errors (8 + 3 = 11).
    mawb_df.loc[mawb_df["MAWB"].notna() & (mawb_df["MAWB"].str.len() == 11), "MAWB"] = (
        mawb_df["MAWB"].str[:3] + "-" + mawb_df["MAWB"].str[-8:]
    )

    return mawb_df

def process_shipper_site_dataframe(shipper_site_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the Shipper Site dataframe and returns it.
    The resulting dataframe is filtered to only contain the following columns:
    - Job Number
    - House Waybill Number
    - Temperature Range
    - Qualification Time
    - Ship Unit Quantity
    - Ship Unit Weight
    - Target Delivery Consignee
    - Consignee City

    Parameters
    ----------
    shipper_site_df:
        The Shipper Site dataframe

    Returns
    -------
    pd.DataFrame:
        The Shipper Site xls file as a pandas dataframe.
    """
    select = [
        "Load #",
        "Ref: House Waybill Number",
        "Ref: Temperature Range",
        "ActPlan: Act Plan: Qualification Time",
        "Actual Ship Unit Quantity",
        "Actual Ship Unit Weight",
        "Target Delivery (Range)",
        "Consignee City",
    ]
    shipper_site_df = shipper_site_df.loc[:, select]
    shipper_site_df.rename(columns={
        "Load #": "Job Number", 
        "Ref: House Waybill Number": "House Waybill Number", 
        "Ref: Temperature Range": "Temperature Range",
        "ActPlan: Act Plan: Qualification Time": "Qualification Time",
        "Actual Ship Unit Quantity": "Ship Unit Quantity",
        "Actual Ship Unit Weight": "Ship Unit Weight",
        "Target Delivery (Range)": "Target Delivery Consignee",
    }, inplace=True)

    # Capitalize the first letter of each word in the "Consignee City" column.
    shipper_site_df["Consignee City"] = shipper_site_df["Consignee City"].apply(lambda x: x.title() if isinstance(x, str) else x)

    return shipper_site_df
    
def create_virtual_import_board_dataframe(mawb_df: pd.DataFrame, shipper_site_df: pd.DataFrame, consolidate: bool = False) -> pd.DataFrame:
    """
    Creates the virtual import board dataframe and returns it.

    Parameters
    ----------
    mawb_df:
        The processed MAWB dataframe
    
    shipper_site_df:
        The processed Shipper Site dataframe
    
    Returns
    -------
    pd.DataFrame:
        The virtual import board dataframe.
    
    """

    # Process the dataframes
    mawb_df = process_mawb_dataframe(mawb_df, consolidate)
    shipper_site_df = process_shipper_site_dataframe(shipper_site_df)

    vib_df = pd.merge(mawb_df, shipper_site_df, left_on="Job Number", right_on="Job Number", how="inner")
    return vib_df