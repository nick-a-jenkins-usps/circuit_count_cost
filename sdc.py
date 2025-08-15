""" SDC class is used to capture specfic logic for SDC sites and their unique statuses"""
import time
import asyncio
import pandas as pd
from server import Server
from temp import Tipne, Site


class Sdc:
    """
    The SDC class is used to store FDB IDs that are SDCs to filter out from the site list.
    """
    def __init__(self):
        self.server: Server = Server()
        self.tipne: Tipne = Tipne()
        self.site_list: Site = Site()
        self.tipne.get_tipne()
        self.site_list.get_site_list()
        # Hard coded list determined manually
        self.sdc_fdbs: list= [
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
        self.sdc_server_status_df: pd.DataFrame = pd.DataFrame()
        self.sdc_df: pd.DataFrame = pd.DataFrame()
        self.merged_df = asyncio.run(self.main())
        print(self.merged_df.columns)

    """
        Attempted async function calls to improve run time with network IO operations
        No known benefit perceived with tests of syncrhonous calls
    """
    async def get_statuses(self) -> None:
        """
        Runs the server class's get_sdc_site_tracking method to get latest
        vendor statuses for SDC sites.

        returns: None but sets the member attribute sdc_server_status_df
        to the df of the returned file
        """
        temp_df: pd.DataFrame= self.server.get_sdc_site_tracking()
        temp_df['fdbid'] = temp_df['fdbid'].astype(str)
        self.sdc_server_status_df = temp_df

    async def filter_site_list(self):
        """
        Async method to filter to the SDC sites from the site list attribute

        returns: None but sets the sdc_df attribute to the trimmed dataframe
        """
        temp_df = self.site_list.site_list_df
        # Convert to string for merges. Nulls prevent int casting
        str_sdc_fdbs = [str(fdb) for fdb in self.sdc_fdbs] 
        self.sdc_df = temp_df[temp_df['fdbid'].isin(str_sdc_fdbs)]

    async def main(self, merge_on='fdbid'):
        """
         call to run the filter_site_list and get_statuses methods.
         chosen due to network IO. No time saved however
         returns: pd.DataFrame of the merged sdc_df with the server statuses
        """
        await self.get_statuses()
        await self.filter_site_list()
        return pd.merge(self.sdc_df, self.sdc_server_status_df, on=merge_on, how='left')

    def get_counts(self):
        """
        Gets the counts from get_assigned_counts and get_deployed_counts methods
        and merges the dfs together.
        returns: pd.DataFrame of the merged counts
        """

        assigned = self.get_assigned_counts(self.merged_df)
        deployed = self.get_deployed_counts(self.merged_df)
        return pd.merge(assigned ,deployed, on='new_vendor', how='left').fillna(0)

    def get_assigned_counts(self, df):
        """
        Calculates number of unique fdbs assigned by vendor

        returns pd.DataFrame of vendor assigned counts
        """
        # nunique() is used to get the number of unique sites for a vendor
        return df.groupby('new_vendor')['fdbid'].nunique().reset_index()

    def get_deployed_counts(self, df):
        """
        Calculates deployed sites using the statsues obtained from the server method

        returns: pd.DataFrame with deployed counts
        """
        # vdr_status and vendor status come from Server method
        deployed = df[
            (df['vdr_status_1'].str.lower().str.contains('complete')) |
            (df['vdr_status_2'].str.lower().str.contains('complete')) |
            (df['vendor_status'].str.lower().str.contains('complete'))
        ]
        # nunique() is chosent to get the number of unique sites for vendors
        return deployed.groupby('new_vendor')['fdbid'].nunique().reset_index()

    def get_costs(self):
        """
        Calls the leg_costs and cur_costs methods and merges them into one df of costs

        returns: pd.DataFrame of merged costs
        """
        leg_costs: pd.DataFrame = self.get_legacy_costs(self.merged_df)
        cur_costs: pd.DataFrame = self.get_current_costs(self.merged_df)
        return pd.merge(leg_costs, cur_costs, on='new_vendor', how='left').reset_index().fillna(0)

    def get_legacy_costs(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the legacy costs of the circuits using legacy_yearly_cost

        returns: pd.DataFrame of legacy costs
        """
        return df.groupby('new_vendor')['legacy_yearly_cost'].sum().reset_index()

    def get_current_costs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates current costs by summing yearly_cost

        returns: pd.DataFrame of current costs
        """
        return df.groupby('new_vendor')['yearly_cost'].sum().reset_index()

    def merge_count_costs(self) -> pd.DataFrame:
        """
        Merges the counts and costs into the final product to be used in the table

        returns pd.DataFrame for the final table
        """
        return pd.merge(self.get_counts(), self.get_costs(), on='new_vendor', how='left')


if __name__ == '__main__':
    start = time.time()
    sdc = Sdc()
    print(sdc.merge_count_costs())
