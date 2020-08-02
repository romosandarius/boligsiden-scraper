from geopy.geocoders import Nominatim
from tqdm import tqdm

# Preprocess
class Preprocessor(object):

    def process(self, df):

        # Convert numeric columns to float
        df = self._clean_columns(df)
        
        # Add geodata (Nominatim is shit)
        #df = self._add_geodata(df)

        return df

    
    def _clean_columns(self, df):
        # Convert numeric columns to float
        cols = ['paymentCash', 'downPayment', 'paymentExpenses', 'paymentGross','paymentNet', 'areaResidential', 
                'numberOfRooms', 'areaParcel', 'salesPeriod', 'areaPaymentCash', 'areaWeighted', 'salesPeriodTotal']

        def if_dash(x):
            if not x.isnumeric():
                x = 0
            return x

        for col in cols:
            df[col] = df[col].apply(lambda x: x.replace('.', ''))
            df[col] = df[col].apply(lambda x: if_dash(x))
            df[col] = df[col].astype(float)

        return df
    
    
    def _add_geodata(self, df):
        
        locator = Nominatim(user_agent='myGeocoder')
        
        address = list(df['address'])
        city = list(df['city'])
        latitude = []
        longitude = []
        
        for i in tqdm(range(len(address))):     
            location = locator.geocode('{}, {}, Denmark'.format(address[i], city[i]))
            
            if location == None:
                latitude.append(None)
                longitude.append(None)
            else:
                latitude.append(location.latitude)
                longitude.append(location.longitude)

        df['latitude'] = pd.Series(latitude)
        df['longitude'] = pd.Series(longitude)
                
        return df

    