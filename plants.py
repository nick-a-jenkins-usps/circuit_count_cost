from server import Server
from temp import ClassInterface, Tipne
import pandas as pd

class Plant:
    def __init__(self) -> None:
        self.server = Server()
        self.interface = ClassInterface()
        self.tipne = Tipne()
        self.tipne.get_tipne()
        self.interface.add_cutover_column()
        self.plant_df = None
    

    def get_final_plant_df(self):
        counts = self.group_by_vendor()
        costs = self.get_financials()
        return (pd.merge(counts, costs, left_on='new_provider', right_on='new_vendor', how='left')
                .drop(['new_vendor'], axis=1)
        )
    
    def get_plants(self):
        return (self.interface.merged_sl_tipne
                [self.interface.merged_sl_tipne['phase'] == '2']
        ).drop_duplicates()
    

    def merge_tipne(self):
        tipne_df = self.server.run()
        plants = self.tipne.tipne_df
        # Drop old_service_number to make circuits that are the same appear the same
        # Drop duplicates to drop the same circuits
        plants = (plants[plants['phase'] == '2']
                  .drop(['old_service_number'], axis=1)
                  .drop_duplicates()
        )
        self.plants_df = plants['fdbid'].unique()

        return pd.merge(plants, tipne_df, on='fdbid', how='left')
    
    def group_by_vendor(self):
        df = self.merge_tipne()
        assigned_count = df.groupby('new_provider')['fdbid'].count().reset_index()
        completed_count = self.group_by_vendor_complted(df)
        return (pd.merge(assigned_count, completed_count, on='new_provider', how='left')
                .rename(columns={'fdbid_x': 'Assigned', 'fdbid_y': 'Deployed'})
        )
    
    def group_by_vendor_complted(self, df):
        completed_statuses = ['Cutover One Circuit Only - Complete', 'Cutover Full - Complete']
        completed_statuses_lower = [status.lower() for status in completed_statuses]
        completed_df = df[df['status'].str.lower().isin(completed_statuses_lower)]
        return completed_df.groupby('new_provider')['fdbid'].count().reset_index()

    def get_financials(self):
        site_list = self.interface.site_list.site_list_df
        plant_list = site_list[site_list['fdbid'].isin(self.plants_df)]
        return self.merge_costs(plant_list)
    
    def merge_costs(self, df):
        new_cost = self.get_yearly_sum(df)
        legacy_cost = self.get_legacy_cost(df)
        return (pd.merge(legacy_cost, new_cost, on='new_vendor', how='left')
                .rename({'new_vendor': 'new_provider',
                         'yearly_cost': 'TIPNe Cost', 
                         'legacy_cost': 'Legacy Cost'}))
    
    def get_yearly_sum(self, df:pd.DataFrame):
        return (df.groupby('new_vendor')['yearly_cost']
                .sum()
                .reset_index()
        )
    
    def get_legacy_cost(self, df):
        return (df.groupby('new_vendor')['legacy_yearly_cost']
                .sum()
                .reset_index()
        )