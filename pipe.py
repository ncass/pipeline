import luigi
import pandas as pd

class mft(luigi.Task):

    def output(self):
        return luigi.LocalTarget('dataset.xlsx')
    
    def run(self):
        y = pd.read_csv('/Users/nicholascassara/Documents/Github Projects/Portfolio/APIs/pipeline/inputs/archive/anime.csv')
        with pd.ExcelWriter('/Users/nicholascassara/Documents/Github Projects/Portfolio/APIs/pipeline/dataset.xlsx') as writer:
            y.to_excel(writer, sheet_name='rawdata', index='true')
            y.head().to_excel(writer, sheet_name='top-sample', index='true')
            y.tail().to_excel(writer, sheet_name='bot-sample', index='true')
            y.describe().to_excel(writer, sheet_name='metrics', index='true')

#second task to extract just the anime_id and name to save as a reference/lookup table for later
class mst(luigi.Task):

    def requires(self):
        return mft()

    def output(self):
        return luigi.LocalTarget('lookupTable.xlsx')
    
    def run(self):
        x = pd.DataFrame(data=pd.read_excel('dataset.xlsx'))
        print('\n',x[['anime_id', 'name']],'\n')
        with pd.ExcelWriter('/Users/nicholascassara/Documents/Github Projects/Portfolio/APIs/pipeline/lookupTable.xlsx') as writer:
            x[['anime_id', 'name']].to_excel(writer, sheet_name='id code', index='true')

class mtt(luigi.Task):

    def requires(self):
        return mst()
    
    def output(self):
        return luigi.LocalTarget('refined_list.xlsx')
    
    def run(self):
        x = pd.DataFrame(data=pd.read_excel('dataset.xlsx'))
        desired_rating = x['rating'].mean() * 1.10
        y = x.where(x['rating'].astype('float') > desired_rating)
        y.dropna(inplace=True)
        with pd.ExcelWriter('/Users/nicholascassara/Documents/Github Projects/Portfolio/APIs/pipeline/refined_list.xlsx') as writer:
            y.to_excel(writer, sheet_name='highly_rated', index='true')

if __name__ == '__main__()':
    luigi.run()