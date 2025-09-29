import asyncio
import aiohttp
import os
from tqdm import tqdm
import csv

# Constants
CHUNK_SIZE = 100000
OUTPUT_DIR = "chunks"
TOP_N = 1000000
semaphore = asyncio.Semaphore(125)  # Limit concurrency

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Fetch a single project
async def fetch(session, project_id):
    async with semaphore:
        url = f"https://api.scratch.mit.edu/projects/{project_id}"
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'ID': project_id,
                        'Title': data['title'],
                        'Creator': data['author']['username'],
                        'Views': data['stats']['views'],
                    }
        except Exception:
            pass
    return None

# Crawl a range of projects
async def crawl_projects(start_id, end_id, output_file='top_scratch_projects.csv', global_bar=None):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch(session, pid)) for pid in range(start_id, end_id + 1)]
        results = []
        chunk_bar = tqdm(total=len(tasks), desc=f"Crawling Scratch Projects {start_id}–{end_id}", position=1)
        
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                results.append(result)
            chunk_bar.update(1)
            if global_bar:
                global_bar.update(1)
        
        chunk_bar.close()

    # Sort and save top N projects
    results.sort(key=lambda x: x['Views'], reverse=True)
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['ID', 'Title', 'Creator', 'Views'])
        writer.writeheader()
        writer.writerows(results[:TOP_N])
    print(f"📦 Done! Saved top {TOP_N} projects to {output_file}")

# Run chunks with global progress bar
async def run_chunks(start_id, end_id):
    total_ids = end_id - start_id + 1
    with tqdm(total=total_ids, desc="🔄 Total Crawl Progress", position=0) as global_bar:
        chunk_num = 1
        for start in range(start_id, end_id + 1, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE - 1, end_id)
            filename = os.path.join(OUTPUT_DIR, f"top_{chunk_num:10}.csv")
            await crawl_projects(start, end, output_file=filename, global_bar=global_bar)
            chunk_num += 1

# Merge all chunk files into one leaderboard
def merge_chunks():
    all_projects = []
    for file in sorted(os.listdir(OUTPUT_DIR)):
        if file.endswith(".csv"):
            with open(os.path.join(OUTPUT_DIR, file), newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_projects.extend(list(reader))

    all_projects.sort(key=lambda x: int(x['Views']), reverse=True)
    with open("top_scratch_projects.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['ID', 'Title', 'Creator', 'Views'])
        writer.writeheader()
        writer.writerows(all_projects[:TOP_N])
    print(f"\n🏁 Final leaderboard saved to top_scratch_projects.csv")

# Entry point
if __name__ == "__main__":
    try:
        start_id = int(input("🔢 Enter the first project ID to crawl (e.g., 104): "))
        end_id = int(input("🔢 Enter the final project ID to crawl (e.g., 10000): "))

        if end_id - start_id > CHUNK_SIZE:
            try:
                asyncio.run(run_chunks(start_id, end_id))
                merge_chunks()
            except KeyboardInterrupt:
                print("\n🛑 Crawl interrupted.")
        else:
            try:
                asyncio.run(crawl_projects(start_id, end_id))
            except KeyboardInterrupt:
                print("\n🛑 Crawl interrupted.")
    except ValueError:
        print("⚠️ Please enter a valid integer.")
