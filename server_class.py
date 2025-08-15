"""The server class handles logic to get the latest files from the TelcoInv Server that houses the Plant and SDC statuses that are the most accurate
"""
import os
from datetime import datetime
from pathlib import Path
import pandas as pd


class Server:
    def __init__(self) -> None:
        self.msp_df: pd.DataFrame = pd.DataFrame()
        # Param dict is used to pass the file prefix name later in function calls
        self.param_dict: dict = {
            'Granite': 'Granite_site_tracking_special_projects',
            'Lumen': 'Lumen_site_tracking_sp',
            'Comcast': 'Comcast_site_tracking',
            'Verizon': 'Verizon_site_tracking',
            'Hughes': 'Hughes_site_tracking',
        }

    def get_latest_file(self, param='msp'):
        """
        Get's the newest file based on the param passed in. The function call is used for both
        msp (plant class) and site_trackign (sdc class)

        returns: File Path.
        """
        server = Path(r"\\eagnmntwe1660\TelcoInv")
        potential_files: list = ([file for file in server.iterdir()
                                  if param.lower() in str(file).lower() and
                                  'leo' not in str(file).lower()]
        )
        return max(potential_files, key=os.path.getmtime) if potential_files else None

    def get_fdb_status(self):
        """
        Gets the status for plants (phase 2) from the server. Calls the get_latest_file() function.

        returns pd.DataFrame with the status of the plant
        """
        latest_file = self.get_latest_file()
        if latest_file:
            # dtype = str used due to inconsistencies in fdbid
            df = pd.read_csv(latest_file, encoding='latin1', dtype=str)
            df['Vdr_Status'] = df['Vdr_Status'].str.strip().str.lower()
            return df[['FDB ID', 'Date_Truck Roll 2/MSP_Cmplt', 'Vdr_Status']]
        else:
            print("No MSP file found.")
            return pd.DataFrame()

    def get_sdc_site_tracking(self):
        """
        Gets the sdc statuses from the server by using the param dictionary established upon
        instantiation

        returns: pd.DataFrame with sdc statuses
        """

        sp_files: list = []
        for vendor, value in self.param_dict.items():
            param: str = self.param_dict[vendor]
            file:str = str(self.get_latest_file(param))
            sdc_status_df: pd.DataFrame = pd.read_csv(file, encoding='latin1', dtype=str)
            sdc_status_df.rename(columns={
                'FDB_ID': 'fdbid',
                'Circuit1_Vdr_Status': 'vdr_status_1',
                'Circuit2_Vdr_Status': 'vdr_status_2',
                'FDB': 'fdbid',
                'Vendor_Status': 'vendor_status',
            }, inplace=True)

            try:
                slim_sdc:pd.DataFrame = sdc_status_df[['fdbid', 'vdr_status_1', 'vdr_status_2']]
            except KeyError:
                slim_sdc = sdc_status_df[['fdbid', 'vendor_status']]

            sp_files.append(slim_sdc)

        final_df: pd.DataFrame = pd.concat(sp_files, axis=0, ignore_index=True)
        final_df = (final_df.dropna(subset=[
            'vdr_status_1',
            'vdr_status_2',
            'vendor_status'],
            how='all'))
        final_df['fdbid'] = final_df['fdbid'].fillna('0').astype(int)
        return final_df

    def run(self):
        """
        Runs the get_fdb_status() method and formats the plants df

        returns: pd.DataFrame with plants
        """
        plants = self.get_fdb_status()
        null_date = datetime(2099, 1, 1).date()
        plants = (plants
                  .fillna({'Date_Truck Roll 2/MSP_Cmplt': null_date})
                  .dropna(subset=['FDB ID'])
        )
        # plants = msp.dropna(subset=['FDB ID'])
        mask = ((plants['Vdr_Status'] == 'cutover one circuit only - complete') &
                (plants['Date_Truck Roll 2/MSP_Cmplt'] == datetime(2099, 1, 1).date())
        )
        plants.loc[mask, 'Date_Truck Roll 2/MSP_Cmplt'] = datetime(2025,1,1).date()
        plants.rename(columns={'FDB ID': 'fdbid'}, inplace=True)
        return plants

    def get_num_cutover_complete(self):
        """
        Test function to determine validity of the run() function
        Can be used for adhoc requests.

        returns int with number of cutovers.
        """
        df = self.run()
        return len(df[df['Vdr_Status'].str.lower()
                       .isin(['complete',
                             'cutover one circuit only - complete',
                             'cutover full - complete'])]
        )


if __name__ == '__main__':
    server_ = Server()
    temp = server_.run()
    # print(df)
