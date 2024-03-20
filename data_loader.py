import cudf
from utils.s3utils import S3Utils
import io
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

class JsonToGDFLoader:
    def __init__(self, folder_url):
        self.folder_url = folder_url
        self.s3utils = S3Utils()

    def download_and_load_json(self, url):
        content = self.s3utils.download_file_get_content(url)
        if content:
            return cudf.read_json(io.BytesIO(content), lines=True)
        return None

    def load_jsons_to_df_with_cudf(self, urls, max_workers=4):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            gdf_futures = {executor.submit(self.download_and_load_json, url): url for url in urls}

        gdf_list = []
        for future in as_completed(gdf_futures):
            gdf = future.result()
            if gdf is not None:
                gdf_list.append(gdf)

        full_gdf = cudf.concat(gdf_list, ignore_index=True) if gdf_list else cudf.DataFrame()
        return full_gdf

    def load(self):
        list_urls = self.s3utils.list_files(self.folder_url)
        max_workers = os.cpu_count() - 1 or 1
        gdf = self.load_jsons_to_df_with_cudf(list_urls, max_workers)
        return gdf

    def print_head_and_time_taken(self):
        st_time = time.time()
        gdf = self.load()
        print(len(gdf))
        print(f"Time taken: {time.time() - st_time}")

# Usage
folder_url = 'data_testing/7bc4e498-e698-11ee-9eb5-025f58bc16f6'
loader = JsonToGDFLoader(folder_url)
loader.print_head_and_time_taken()
