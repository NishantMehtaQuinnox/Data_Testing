from concurrent.futures import ThreadPoolExecutor, as_completed
import cudf
from utils.s3utils import S3Utils
import io
import os
import time
# Instantiate S3Utils
st_time = time.time()
s3utils = S3Utils()
folder_url = 'data_testing/7bc4e498-e698-11ee-9eb5-025f58bc16f6'

list_urls = s3utils.list_files(folder_url)

def download_and_load_json(url):
    content = s3utils.download_file_get_content(url)
    if content:
        return cudf.read_json(io.BytesIO(content), lines=True)
    return None

def load_jsons_to_df_with_cudf(urls, max_workers=4):
    # Use ThreadPoolExecutor to download files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        gdf_futures = {executor.submit(download_and_load_json, url): url for url in urls}
    
    gdf_list = []
    for future in as_completed(gdf_futures):
        gdf = future.result()
        if gdf is not None:
            gdf_list.append(gdf)
    
    # Concatenate all DataFrames into a single one only if gdf_list is not empty
    full_gdf = cudf.concat(gdf_list, ignore_index=True) if gdf_list else cudf.DataFrame()
    return full_gdf


# Load the JSON files into a single cuDF DataFrame
max_workers = os.cpu_count() - 1 or 1
gdf = load_jsons_to_df_with_cudf(list_urls, max_workers)
print(gdf.head())  # Print the first 5 rows of the DataFrame
print(f"Time taken : {time.time() - st_time}")
