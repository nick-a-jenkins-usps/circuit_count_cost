from temp import ClassInterface
import pandas as pd

class RD:
    def __init__(self):
        self.interface = ClassInterface()
        self.interface.add_cutover_column()
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