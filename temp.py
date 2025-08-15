"""temp.py: This module instantiates Site and Tipne dfs"""
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import numpy as np
from server import Server

@dataclass
class FileManager:
    """
    Abstract class to handle file paths

    """
    def __init__(self):
        self.splunk_path = "C:/Users/FDYPK0/OneDrive - USPS/NCP WAN/Splunk/"
        self.site_list_path = 'site_list.csv'
        self.tipne_path = 'tipne.csv'

class Site(FileManager):
    """
    Class used to handle instantiating the site list and merging tiwht tipne df

    """
    def __init__(self):
        super().__init__()
        self.site_list_df = pd.DataFrame()

    def get_site_list(self):
        """
        Makes assigns the site_list variable to the member variable site_list_df
        """
        site_list = pd.read_csv(self.splunk_path + self.site_list_path)
        site_list = self.format_site_list(site_list)
        self.site_list_df = site_list

    def format_site_list(self, site_list: pd.DataFrame):
        """
        Formats the fdbid, nrc, and mrc columns into format and dytpes needed
        """
        # fdbid formatted as a string to allow for '-' in column
        site_list['fdbid'] = site_list['fdbid'].replace('TBD', np.nan).dropna().astype(str)
        site_list = self.format_nrc(site_list).pipe(self.format_old_mrc)
        site_list = self.format_mrc(site_list).pipe(self.add_yearly_cost)
        return site_list

    def format_mrc(self, site_list: pd.DataFrame):
        """
        Formats that mrc as a float
        """
        site_list['mrc'] = site_list['mrc'].astype(float)
        return site_list

    def add_yearly_cost(self, site_list: pd.DataFrame):
        """
        Calculates the yearly cost and creates a column
        """
        site_list['yearly_cost'] = site_list['mrc'] * 12
        return site_list

    def format_old_mrc(self, site_list: pd.DataFrame):
        """
        Formats old mrc as a numerical column that can be used in computations
        """
        site_list['old_mrc'] = site_list['old_mrc'].replace('-', np.nan).dropna().astype(float)
        site_list['old_mrc'] = site_list['old_mrc'].fillna(site_list['old_mrc'].mean())
        site_list = self.add_yearly_legacy_cost(site_list)
        return site_list

    @staticmethod
    def add_yearly_legacy_cost(site_list: pd.DataFrame):
        """
        Creates the column legacy_yearly_cost and returns the dataframe
        """
        site_list['legacy_yearly_cost'] =  site_list['old_mrc'] * 12
        return site_list

    @staticmethod
    def format_nrc(site_list: pd.DataFrame):
        """
        Formats NRC column by extracting digits, replacing commas,
        and filling nulls with the mean value
        """

        site_list['nrc'] = (site_list['nrc'].astype(str)
                            .str.extract(r'([\d,]+\.\d{2})')[0]
                            .str.replace(',', '', regex=True)
                            .astype(float)
        )

        # Causes a TypeError if performed in line above
        site_list['nrc'] = site_list['nrc'].fillna(site_list['nrc'].mean())
        return site_list

class Tipne(FileManager):
    """
    Class used to make the tipne project tracking dataframe
    """
    def __init__(self):
        super().__init__()
        self.tipne_df = pd.DataFrame()

    def get_tipne(self):
        """
        Gets the tipne project tracking from the file path
        """
        tipne = pd.read_csv(self.splunk_path + self.tipne_path, low_memory=False)
        # Future date needed to prevent counting as a cutover.
        tipne['cutover_completed_date'].fillna(datetime(2099, 1, 1), inplace=True)
        self.tipne_df = tipne

    def get_phase_dict(self):
        """
        Unsure of importance of function yet
        """
        phases = self.tipne_df['phase'].unique()
        return {phase: self.tipne_df[self.tipne_df['phase'] == phase] for phase in phases}
    

class Sdc:
    """
    The SDC class is used to store FDB IDs that are SDCs to filter out from the site list.
    """
    def __init__(self):
        self.sdc_fdbs= [
                1589779, 1388480, 1599136, 1578072, 1578792, 1582789, 1599154, 1582791, 1599160,
                1579662, 1582794, 1578073, 1579657, 1578793, 1599153, 1579664, 1579654, 1582556,
                1599157, 1582554, 1582785, 1582543, 1579658, 1582778, 1582133, 1582550, 1599158,
                1582129, 1599146, 1582837, 1585537, 1599155, 1582542, 1594603, 1594598, 1578797,
                1579666, 1582135, 1578794, 1582549, 1589776, 1582790, 1579659, 1582553, 1436550,
                1587292, 1582128, 1582781, 1599137, 1582551, 1589778, 1582131, 1582783, 1578796,
                1582777, 1599147, 1579667, 1579660, 1582126, 1579653, 1582793, 1582541, 1599151,
                1578798, 1583378, 1599149, 1578795, 1578074, 1582548, 1582130, 1583040, 1599161,
                1594602, 1599152, 1582780, 1579663, 1579652, 1583380, 1582788, 1582127, 1577752,
                1594600, 1594597, 1599145, 1582555, 1451470, 1583039, 1582546, 1574856, 1582124,
                1352332, 1582123, 1582786, 1587280, 1587281, 1594601, 1594605, 1599156, 1599144,
                1601568, 1434108, 1594599, 1579656, 1579665, 1578075, 1579661, 1582552, 1582132,
                1594596, 1566914, 1578799, 1599138,
            ]

        # self.sdc_df = self.site_list.site_list_df[self.site_list.site_list_df['fdbid'].astype(int).isin(self.sdc_fdbs)]


class ClassInterface:
    """
    Encapsulates the instantiations and sequence of running the classes.
    """
    def __init__(self):
        self.site_list = Site()
        self.tipne = Tipne()
        self.sdc = Sdc()
        self.merged_sl_tipne = pd.DataFrame()

    def initiate(self):
        """
        Initiates the runs of the Site and Tipne objects that assign their
        respective member variables
        """
        self.site_list.get_site_list()
        self.tipne.get_tipne()

    def merge_tipne_site_list(self):
        """
        Initiates the Site and Tipne class and merged them
        """
        self.initiate()  # Needed to make the respective classes run
        merged_df = (pd.merge(self.tipne.tipne_df[['fdbid', 'new_provider', 'phase','status']],
                              self.site_list.site_list_df[['fdbid', 'mrc', 'nrc', 'old_mrc', 
                                                           'yearly_cost', 'legacy_yearly_cost']],
                              on='fdbid',
                              how='left')
        )
        return merged_df

    def add_cutover_column(self):
        """
        Takes the merged df from the merge_tipne_site_list function
        and adds boolean cutover column for computations
        """
        merged_df = self.merge_tipne_site_list()

        merged_df['is_cutover'] = (merged_df['status']
                .str.lower()
                .str.contains(
                            'cutover complete|' \
                            'cutover one circuit only|' \
                            'cutover mc complete', regex=True
                            )
        )
        self.merged_sl_tipne = merged_df

class RD:
    def __init__(self):
        self.interface = ClassInterface()
        self.interface.add_cutover_column()
        self.sdc = Sdc()
        self.rd_df = pd.DataFrame()


    def filter_by_cutover(self, rd_df):
        return rd_df[rd_df["is_cutover"] == True]

    def get_vendor_counts(self, rd_df: pd.DataFrame, col_rename: str):
        return rd_df.groupby('new_provider')['fdbid'].count().reset_index().rename(columns={'fdbid': col_rename})
    
    def get_yearly_cost_by_vendor(self, rd_df:pd.DataFrame, mrc_col: str):
        return rd_df.groupby('new_provider')[mrc_col].sum()
    
    def get_rd_df(self):
        df = self.interface.merged_sl_tipne
        phases = ['1', '3', '4', 'SP','LEO'] # These numbers match up with Tipne Project Tracking
        slim_df = df[(df['phase'].isin(phases)) & ~df['fdbid'].isin(self.interface.sdc.sdc_fdbs)]
        self.rd_df = slim_df

    def run_phases(self):
        phases = [['4'], ['LEO'], ['1', '3', 'SP']]
        df_list = [self.get_phase_cost_counts(phase) for phase in phases] 
        return self.merge_list(df_list)

    def merge_list(self, df_list):
        init_df = df_list[0]
        rest_df = df_list[1:]
        for df in rest_df:
            init_df = self.merge_abstract(init_df, df)
        return init_df

    def get_phase_cost_counts(self, phase:list):
        phase_df = self.rd_df[(self.rd_df['phase'].isin(phase)) & (~self.rd_df['fdbid'].isin(self.sdc.sdc_fdbs))]
        count = self.get_vendor_counts(phase_df, 'Count')
        cost = self.get_yearly_cost_by_vendor(phase_df, 'yearly_cost')
        return self.merge_abstract(count, cost)
        

    def get_assigned_deployed_counts(self):
        assign_counts = self.get_vendor_counts(self.rd_df, 'assigned')
        cutover_counts = self.get_vendor_counts(self.filter_by_cutover(self.rd_df), 'deployed')
        return assign_counts, cutover_counts
    
    def get_legacy_costs(self):
        return self.get_yearly_cost_by_vendor(self.rd_df, 'legacy_yearly_cost')
    
    def merge(self):
        assigned, deployed = self.get_assigned_deployed_counts()
        legacy_cost = self.get_legacy_costs()
        counts = pd.merge(assigned, deployed, on='new_provider', how='left')
        counts_cost = pd.merge(counts, legacy_cost, on='new_provider', how='left')
        return self.merge_abstract(counts_cost, self.run_phases()).fillna(0)
    
    @staticmethod
    def merge_abstract(df_1, df_2):
        return pd.merge(df_1, df_2, on='new_provider', how='outer')


class Plant:
    def __init__(self) -> None:
        self.server = Server()
        self.interface = ClassInterface()
        self.tipne = Tipne()
        self.tipne.get_tipne()
        self.interface.add_cutover_column()
        self.plant_df = None
    
    def get_plants(self):
        return (self.interface.merged_sl_tipne
                [self.interface.merged_sl_tipne['phase'] == '2']
        ).drop_duplicates()
    
    def merge_tipne(self):
        tipne_df = self.server.run()
        plants = self.tipne.tipne_df
        plants = plants[plants['phase'] == '2']
        print(plants)
        return pd.merge(plants.drop_duplicates(), tipne_df, on='fdbid', how='left')
    
    def group_by_vendor(self):
        df = self.merge_tipne()
        print(df['new_provider'].unique())
        return df.groupby('new_provider')['fdbid'].count()

if __name__ == '__main__':
    plants = Plant()
    print(plants.group_by_vendor())
    # holder = RD()
    # holder.get_rd_df()
    # print(holder.merge())
    # print(holder.run_phases())
    # dfs = ClassInterface()
    # dfs.add_cutover_column()
    # df = dfs.merged_sl_tipne
    # print(df[df['phase_y'] == 'Phase LEO'])