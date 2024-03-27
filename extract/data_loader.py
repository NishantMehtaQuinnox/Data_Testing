# import cudf
# from utils.s3utils import S3Utils
# import io
# import os
# import time
# from concurrent.futures import ThreadPoolExecutor, as_completed

# class JsonToGDFLoader:
#     def __init__(self, folder_url):
#         self.folder_url = folder_url
#         self.s3utils = S3Utils()

#     def download_and_load_json(self, url):
#         content = self.s3utils.download_file_get_content(url)
#         if content:
#             return cudf.read_json(io.BytesIO(content), lines=True)
#         return None

#     def load_jsons_to_df_with_cudf(self, urls, max_workers=4):
#         with ThreadPoolExecutor(max_workers=max_workers) as executor:
#             gdf_futures = {executor.submit(self.download_and_load_json, url): url for url in urls}

#         gdf_list = []
#         for future in as_completed(gdf_futures):
#             gdf = future.result()
#             if gdf is not None:
#                 gdf_list.append(gdf)

#         full_gdf = cudf.concat(gdf_list, ignore_index=True) if gdf_list else cudf.DataFrame()
#         return full_gdf

#     def load(self):
#         list_urls = self.s3utils.list_files(self.folder_url)
#         max_workers = os.cpu_count() - 1 or 1
#         gdf = self.load_jsons_to_df_with_cudf(list_urls, max_workers)
#         return gdf
    
#     def transform_gdf(self, gdf, transformations):
#         for transform in transformations:
#             from_column = transform.get('from_column_name')
#             to_column = transform.get('to_column_name')
#             type_update = transform.get('type_update')  # Get the type if it exists
#             transformation_function = transform.get('transformation_function')

#             if transformation_function and from_column in gdf.columns:
#                 gdf[to_column] = gdf[from_column].applymap(transformation_function)

#             # Rename the column if necessary
#             if from_column in gdf.columns and from_column != to_column:
#                 gdf = gdf.rename(columns={from_column: to_column})
                
#             # Update column type if type_update is provided
#             if type_update and to_column in gdf.columns:
#                 gdf[to_column] = gdf[to_column].astype(type_update)
        
#         return gdf




#     def print_head_and_time_taken(self):
#         st_time = time.time()
#         gdf = self.load()
#         print(len(gdf))
#         print(f"Time taken: {time.time() - st_time}")
        
import cudf
from utils.s3utils import S3Utils
import io
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
import logging
from tqdm import tqdm
import pandas as pd
import gc
# Set up the basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3utils = S3Utils()

class JsonToGDFLoader:
    """
    Class to load JSON data from S3 into cuDF dataframes, perform transformations,
    and concatenate into a single dataframe.
    """

    def __init__(self, folder_url):
        """
        Initialize the loader with the folder URL.
        """
        self.folder_url = folder_url

    def download_and_load_json(self, url):
        """
        Download a single JSON file content and load into a cuDF DataFrame.
        """
        try:
            content = s3utils.download_file_get_content(url)
            if content:
                return pd.read_json(io.BytesIO(content), lines=True)
        except Exception as e:
            logger.error(f"Failed to download or load json from {url}, error: {e}")
        return None

    def load_jsons_to_df_with_cudf(self, urls, max_workers=4):
        """
        Parallelize downloads and concatenates individual DataFrames into one.
        """
        futures_to_url = {}
        st_time = time.time()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for url in urls:
                future = executor.submit(self.download_and_load_json, url)
                futures_to_url[future] = url

        # Collecting the results into a list to concatenate later
        df_list = []

        # Wrap the as_completed iterator with tqdm for a progress bar
        for future in tqdm(as_completed(futures_to_url), total=len(urls), unit="file"):
            df = future.result()
            if df is not None:
                print("Adding DF")
                df_list.append(df)
                
        logging.info(f"Downloaded DFs time taken {time.time()-st_time}")
        logging.info(f"AVG time of a single batch {(time.time()-st_time)/len(urls)}")
        
        st_time = time.time()
                
        cudf_dfs = [cudf.DataFrame.from_pandas(df) for df in df_list]

        full_gdf = cudf.concat(cudf_dfs, ignore_index=True) if cudf_dfs else cudf.DataFrame()
        
        logging.info(f"Combined DFs time taken {time.time()-st_time}")
        
        del cudf_dfs,df_list
        gc.collect()
        
        return full_gdf

    def load(self):
        """
        Load JSON files contained within the folder URL into a single cuDF DataFrame.
        """
        list_urls = s3utils.list_files(self.folder_url)
        max_workers = os.cpu_count() - 1 or 1
        return self.load_jsons_to_df_with_cudf(list_urls, max_workers)

    
    def transform_gdf(self, gdf, transformations):
        """
        Apply transformations to a cuDF DataFrame based on a list of transformation rules.
        """
        for transform in transformations:
            from_column = transform.get('from_column_name')
            to_column = transform.get('to_column_name')
            type_update = transform.get('type_update')  # Get the type if it exists
            transformation_function = transform.get('transformation_function')

            # if transformation_function and from_column in gdf.columns:
            #     gdf[to_column] = gdf[from_column].applymap(transformation_function)

            # Rename the column if necessary
            if from_column in gdf.columns and from_column != to_column:
                gdf = gdf.rename(columns={from_column: to_column})
                
            # Update column type if type_update is provided
            if type_update and to_column in gdf.columns:
                gdf[to_column] = gdf[to_column].astype(type_update)
        
        return gdf

    def print_head_and_time_taken(self):
        """
        Load the dataframe, print its head and the total time taken.
        """
        st_time = time.time()
        gdf = self.load()
        logger.info(gdf.head())
        logger.info(f"Time taken: {time.time() - st_time}")

# Initialize your loader and transformations here, and transform the loaded DataFrame
# ...


# # Usage
# folder_url = 'data_testing/7bc4e498-e698-11ee-9eb5-025f58bc16f6'
# loader = JsonToGDFLoader(folder_url)
# gdf = loader.load()


# # Define your transformations list as follows:
# transformations = [
#     {
#         'from_column_name': 'original_column1',
#         'to_column_name': 'new_column1',
#         'type_update': 'int64',
#         'transformation_function': lambda x: x * 2
#     },
#     {
#         'from_column_name': 'original_column2',
#         'to_column_name': 'new_column2',
#         'type_update': None,
#         'transformation_function': lambda x: x.strip() if isinstance(x, str) else x
#     }
#     # Add as many dictionaries as needed for transformations
# ]

# folder_url = 'data_testing/7bc4e498-e698-11ee-9eb5-025f58bc16f6'
# loader = JsonToGDFLoader(folder_url)
# loader.print_head_and_time_taken()
# # gdf = loader.load()
# # tgdf = loader.transform_gdf(gdf,transformations)
