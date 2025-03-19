import sys
import mmap
import xxhash
import json
import multiprocessing
import os
from os import listdir
from os.path import isfile, join
from pathlib import Path
from datetime import datetime
from json import JSONDecodeError
from tqdm import tqdm

def update_progress_bar(progress_queue, total_lines):
    """Runs in a separate process to update the tqdm progress bar safely."""
    with tqdm(total=total_lines, desc="Filtering logs", unit="lines") as pbar:
        processed_lines = 0
        while True:
            try:
                update = progress_queue.get(timeout=1)  # Get updates from workers
                if update == "DONE":
                    break
                processed_lines += update
                pbar.update(update)
            except Exception:
                continue  # Avoid blocking if queue is empty

        # Ensure progress reaches 100%
        pbar.update(total_lines - processed_lines)

class Logstash:
    def __init__(self):
        self.directory = Path(__file__).resolve().parent

    def filter(self, log_file_path: str, filter_file_path: str, overwrite: bool = False) -> dict:
        """Filters logs based on provided JSON filters."""
        log_file = Path(log_file_path)
        filter_file = Path(filter_file_path)

        if not log_file.exists():
            return {"error": f"Log file '{log_file_path}' not found."}
        if not filter_file.exists():
            return {"error": f"Filter file '{filter_file_path}' not found."}

        # Load filter file
        try:
            with open(filter_file, 'r', encoding='utf-8') as f:
                filters = json.load(f)
                filter_keys = set(filters)
        except JSONDecodeError as e:
            return {"error": f"Invalid JSON in filter file: {str(e)}"}

        if not filter_keys:
            return {"warning": "No filter keys found in filter file. Skipping filtering."}

        # Define directory named after the log file (without extension)
        log_folder = self.directory / "../../data/filtered_logs" / log_file.stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = log_folder / f"{log_file.stem}_{timestamp}.filtered.log"
        metadata_file = log_folder / f"{log_file.stem}_{timestamp}.metadata.json"

        # Check if folder exists
        if log_folder.exists() and not overwrite:
            return {"error": f"Folder '{log_folder}' already exists. Use overwrite=True to proceed."}

        # Ensure directory exists
        os.makedirs(log_folder, exist_ok=True)

        # Perform filtering
        matched_lines, filter_counts = self._parallel_filter(log_file, output_file, filter_keys)

        result_payload = {
            "hits": {
                "total": matched_lines,
                **{key: count for key, count in sorted(filter_counts.items(), key=lambda item: item[1], reverse=True) if count > 0}
            },
            "meta": {
                "filter": filter_file.name,
                "filtered_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "file": self.get_info(log_file)
            }
        }

        # Write metadata JSON inside the new log folder
        with open(metadata_file, 'w', encoding="utf-8") as meta:
            json.dump(result_payload, meta, indent=4)

        return result_payload

    def get_info(self, file_path: str) -> dict:
        """Retrieves metadata about a log file."""
        log_file = Path(file_path)

        if not log_file.exists():
            return {"error": f"Log file '{file_path}' not found."}

        return {
            "file_name": log_file.name,
            "file_size": f'{log_file.stat().st_size} bytes',
            "created_at": datetime.fromtimestamp(log_file.stat().st_ctime).strftime("%d-%m-%Y %H:%M:%S"),
            "modified_at": datetime.fromtimestamp(log_file.stat().st_mtime).strftime("%d-%m-%Y %H:%M:%S"),
            "hash": self._compute_file_hash(log_file),
            "lines": self._count_lines(log_file),
        }

    def _compute_file_hash(self, file_path: Path, chunk_size: int = 16 * 1024 * 1024) -> str:
        """Computes an xxhash for the given file efficiently."""
        try:
            hasher = xxhash.xxh3_128()
            with open(file_path, "rb", buffering=0) as f:  # Zero buffering for direct hashing
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            return f'Error computing hash: {str(e)}'

    def _count_lines(self, file_path: Path) -> int:
        """Counts lines efficiently using generator expression."""
        try:
            with open(file_path, 'rb') as f:
                return sum(1 for _ in f)
        except Exception as e:
            return f"Error counting lines: {str(e)}"

    def _parallel_filter(self, log_file: Path, output_file: Path, filter_keys: set):
        """Parallelizes log filtering using multiprocessing."""
        num_workers = min(multiprocessing.cpu_count(), len(filter_keys) or 1)
        manager = multiprocessing.Manager()
        filter_counts = manager.dict()

        with open(log_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
            lines = infile.readlines()
            total_lines = len(lines)
            chunk_size = max(1, total_lines // num_workers)

            with multiprocessing.Pool(num_workers) as pool:
                progress_queue = multiprocessing.Queue()  # Queue for progress updates

                # Start tqdm progress bar process
                progress_updater = multiprocessing.Process(
                    target=update_progress_bar, args=(progress_queue, total_lines)
                )
                progress_updater.start()

                results = []
                for i in range(0, total_lines, chunk_size):
                    chunk = lines[i:i + chunk_size]
                    result = pool.apply_async(
                        self.worker_function,
                        args=((chunk, filter_keys),),
                        callback=lambda res: progress_queue.put(len(res[0]))  # Update progress
                    )
                    results.append(result)

                pool.close()
                pool.join()

                # Collect results
                matched_lines = 0
                for result in results:
                    try:
                        filtered, worker_counts = result.get()
                        outfile.writelines(filtered)
                        matched_lines += len(filtered)

                        # Merge filter counts
                        for key, count in worker_counts.items():
                            filter_counts[key] = filter_counts.get(key, 0) + count
                    except Exception as e:
                        sys.stderr.write(f"Worker error: {e}\n")

                # Notify progress bar to finish
                progress_queue.put("DONE")
                progress_updater.join()

        return matched_lines, dict(filter_counts)

    @staticmethod
    def worker_function(args):
        """Worker function for filtering logs."""
        try:
            lines, filter_keys = args
            filtered_lines = []
            local_counts = {key: 0 for key in filter_keys}

            for line in lines:
                for key in filter_keys:
                    if key in line:
                        filtered_lines.append(line + "\n")
                        local_counts[key] += 1
                        break

            return filtered_lines, local_counts
        except Exception as e:
            return [], f"Error: {str(e)}"
