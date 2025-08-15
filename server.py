import os
import pandas as pd
from pathlib import Path
from datetime import datetime


class Server:
    def __init__(self) -> None:
        self.msp_df: pd.DataFrame = pd.DataFrame()
        self.param_dict: dict = {
            'Granite': 'Granite_site_tracking_special_projects',
            'Lumen': 'Lumen_site_tracking_sp',
            'Comcast': 'Comcast_site_tracking',
            'Verizon': 'Verizon_site_tracking',
            'Hughes': 'Hughes_site_tracking',
        }

    def get_latest_file(self, param='msp'):
        server = Path(r"\\eagnmntwe1660\TelcoInv")
        potential_files: list = [file for file in server.iterdir() if param.lower() in str(file).lower() and 'leo' not in str(file).lower()]
        return max(potential_files, key=os.path.getmtime) if potential_files else None

    def get_fdb_status(self):
        latest_file = self.get_latest_file()
        if latest_file:
            df = pd.read_csv(latest_file, encoding='latin1', dtype=str)
            df['Vdr_Status'] = df['Vdr_Status'].str.strip().str.lower()
            return df[['FDB ID', 'Date_Truck Roll 2/MSP_Cmplt', 'Vdr_Status']]
        else:
            print("No MSP file found.")
            return pd.DataFrame()
        
    def get_sdc_site_tracking(self):
        
        sp_files: list = []
        for vendor in self.param_dict.keys():
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
        final_df = final_df.dropna(subset=['vdr_status_1', 'vdr_status_2', 'vendor_status'], how='all')
        final_df['fdbid'] = final_df['fdbid'].fillna('0').astype(int)
        return final_df
    
    def run(self):
        msp = self.get_fdb_status()
        null_date = datetime(2099, 1, 1).date()
        msp.fillna({'Date_Truck Roll 2/MSP_Cmplt': null_date}, inplace=True)
        msp = msp.dropna(subset=['FDB ID'])
        mask = (msp['Vdr_Status'] == 'cutover one circuit only - complete') & (msp['Date_Truck Roll 2/MSP_Cmplt'] == datetime(2099, 1, 1).date())
        msp.loc[mask, 'Date_Truck Roll 2/MSP_Cmplt'] = datetime(2025,1,1).date()
        msp.rename(columns={'FDB ID': 'fdbid'}, inplace=True)
        return msp
    
    def get_num_cutover_complete(self):
        df = self.run()
        return len(df[df['Vdr_Status'].str.lower()
                       .isin(['complete', 
                             'cutover one circuit only - complete', 
                             'cutover full - complete'])]
        )


if __name__ == '__main__':
    server = Server()
    df = server.get_sdc_site_tracking()
    print(df)
    print(df.columns)