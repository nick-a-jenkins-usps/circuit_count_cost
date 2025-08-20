"""Class used to get counts and cost for plants"""
import pandas as pd
from server_class import Server
from site_tracking import ClassInterface, Tipne

class Plant:
    """
    Phase 2 Sites are considered plants. This class uses the server and class interface
    classes to get the statuses of the sites and then calculates the cost and counts.

    """
    def __init__(self) -> None:
        self.server = Server()
        self.interface = ClassInterface()
        self.tipne = Tipne()
        self.tipne.get_tipne()
        self.interface.add_cutover_column()
        self.plant_df = self.get_plants()
        self.plants_fdb = []


    def filter_to_phase_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract logic to filter a dataframe to return phase 2

        returns: pd.DataFrame
        """
        return df[df['phase'] == '2']

    def get_plants(self):
        """
        gets the plants from the interface object's attribute, merged_sl_tipne

        returns: pd.DataFrame
        """
        return (
            self.filter_to_phase_2(self.interface.merged_sl_tipne)
        ).drop_duplicates()


    def merge_tipne(self):
        """
        merges the tipne_df to the plants_df. The tipne_df comes from the server class

        returns: pd.DataFrame
        """
        tipne_df: pd.DataFrame = self.server.run()
        plants: pd.DataFrame = self.tipne.tipne_df
        # Drop old_service_number to make circuits that are the same appear the same
        # Drop duplicates to drop the same circuits
        plants: pd.DataFrame = (self.filter_to_phase_2(plants)
                  .drop(['old_service_number'], axis=1)
                  .drop_duplicates()
        )
        self.plants_fdb = plants['fdbid'].unique()

        return pd.merge(plants, tipne_df, on='fdbid', how='left')

    def group_by_vendor(self) -> pd.DataFrame:
        """
        Gets the assigned and deployed count grouped by vendor and returns the two merged

        returns: pd.DataFrame
        """
        df: pd.DataFrame = self.merge_tipne()
        assigned_count: pd.DataFrame = df.groupby('new_provider')['fdbid'].count().reset_index()
        completed_count: pd.DataFrame = self.group_by_vendor_complted(df)
        return (pd.merge(assigned_count, completed_count, on='new_provider', how='left')
                .rename(columns={'fdbid_x': 'Assigned', 'fdbid_y': 'Deployed'})
        )

    def group_by_vendor_complted(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filteres the dataframe passed in to get cutover sites and then get the counts grouped by
        vendors.

        returns: pd.DataFrame
        """
        completed_statuses: list = (['Cutover One Circuit Only - Complete',
                                     'Cutover Full - Complete'])
        completed_statuses_lower: list = [status.lower() for status in completed_statuses] # Lowercase for matching
        completed_df: pd.DataFrame = df[df['status'].str.lower().isin(completed_statuses_lower)]
        return completed_df.groupby('new_provider')['fdbid'].count().reset_index()

    def get_financials(self) -> pd.DataFrame:
        """
        uses the interface site object's dataframe and the plant_df to merge the dataframes

        returns pd.DataFrame
        """
        site_list: pd.DataFrame = self.interface.site_list.site_list_df

        # self.plants_fdb houses the phase 2 fdbids needed for filtering
        plant_list: pd.DataFrame = site_list[site_list['fdbid'].isin(self.plants_fdb)]
        return self.merge_costs(plant_list)

    def merge_costs(self, df: pd.DataFrame)-> pd.DataFrame:
        """
        Uses the get_yearly sum and get_legacy_cost functions to create two dataframes
        and then merge those two dataframes for a complete cost dataframe

        returns pd.DataFrame
        """
        new_cost: pd.DataFrame = self.get_yearly_sum(df)
        legacy_cost: pd.DataFrame = self.get_legacy_cost(df)
        return (pd.merge(legacy_cost, new_cost, on='new_vendor', how='left')
                .rename({'new_vendor': 'new_provider',
                         'yearly_cost': 'TIPNe Cost',
                         'legacy_cost': 'Legacy Cost'}))

    def get_yearly_sum(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Groups the dataframe by vendor and sums the yearly_cost column

        returns pd.DataFrame
        """
        return (df.groupby('new_vendor')['yearly_cost']
                .sum()
                .reset_index()
        )

    def get_legacy_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Groups the dataframe by vendor and sums the legacy_yearly_cost"

        returns pd.DataFrame
        """
        return (df.groupby('new_vendor')['legacy_yearly_cost']
                .sum()
                .reset_index()
        )

    def get_final_plant_df(self) -> pd.DataFrame:
        """
        Runs the Plant class to get counts and cost for sites

        returns merged pd.DataFrame of counts and cost
        """
        count: pd.DataFrame = self.group_by_vendor()
        costs: pd.DataFrame = self.get_financials()
        return (pd.merge(count, costs, left_on='new_provider', right_on='new_vendor', how='left')
                .drop(['new_vendor'], axis=1)
        )

if __name__ == '__main__':
    plant = Plant()
    print(plant.get_final_plant_df())
