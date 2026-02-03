import os
import asyncio
import json
import shutil
import requests
from playwright.async_api import async_playwright
from huggingface_hub import HfApi
from tqdm import tqdm

# --- CONFIGURATION ---
REPO_ID = "nwokikeonyeka/pitt-rivers-igbo-collection"
SOURCE_ID = "pitt_rivers"

# The specific URL (Search result for "Igbo", filtered by Photograph, Published)
SEARCH_URL = "https://www.prm.ox.ac.uk/collections-online#/search/simple-search/%2522Igbo%2522/%257B%2522catalogue%2522%253A%257B%2522collection%2522%253A%255B%2522Photograph%2522%255D%252C%2522multimedia.isPublished%2522%253A%255B%2522Yes%2522%255D%257D%257D/1/24/_score/desc/catalogue"

BASE_DIR = "data_pitt_rivers"
DIRS = {
    "images": os.path.join(BASE_DIR, "images"),
    "clean": os.path.join(BASE_DIR, "clean")
}

def setup_directories():
    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

async def handle_popups(page):
    """Dismisses Cookie and Sensitive Content warnings."""
    # 1. Cookie
    try:
        cookie_btn = await page.wait_for_selector('#ccc-recommended-settings', timeout=2000)
        if cookie_btn: await cookie_btn.click()
    except: pass

    # 2. Sensitive Content
    try:
        sensitive_btn = await page.query_selector('button:has-text("Enter Site")')
        if not sensitive_btn:
             sensitive_btn = await page.query_selector('button:has-text("View content")')
        
        if sensitive_btn and await sensitive_btn.is_visible():
            await sensitive_btn.click()
            await page.wait_for_timeout(1000)
    except: pass

async def scrape_pitt_rivers():
    data_buffer = []
    
    async with async_playwright() as p:
        print("🚀 Launching Browser...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()
        
        print(f"🌍 Loading Search Page...")
        await page.goto(SEARCH_URL, timeout=60000)
        await page.wait_for_timeout(5000) 
        await handle_popups(page)
        
        # Scroll to trigger lazy loads
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(2000)

        print("🔍 Extracting item links...")
        item_urls = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(h => h.includes('#/item/'));
        }''')
        
        item_urls = list(set(item_urls))
        print(f"✅ Found {len(item_urls)} items.")
        
        for link in tqdm(item_urls, desc="Scraping Items"):
            try:
                await page.goto(link, timeout=45000)
                await page.wait_for_timeout(3000)
                await handle_popups(page)
                
                # Metadata extraction
                metadata = await page.evaluate('''() => {
                    const data = {};
                    const titleEl = document.querySelector('h1, h2');
                    data.title = titleEl ? titleEl.innerText.trim() : "Untitled";
                    document.querySelectorAll('table tr').forEach(row => {
                        const keyEl = row.querySelector('th');
                        const valEl = row.querySelector('td');
                        if (keyEl && valEl) {
                            let key = keyEl.innerText.trim().toLowerCase().replace(/ /g, '_');
                            data[key] = valEl.innerText.trim();
                        }
                    });
                    return data;
                }''')
                
                # IIIF Image extraction
                img_url = await page.evaluate('''() => {
                    const dlImg = document.querySelector('.download-images img');
                    if (dlImg) return dlImg.src;
                    const mainImg = document.querySelector('.item-image img');
                    return mainImg ? mainImg.src : null;
                }''')
                
                if img_url:
                    item_id = metadata.get("accession_number", link.split("/")[-1])
                    safe_id = item_id.replace(".", "_").replace("/", "-").strip()
                    filename = f"prm_{safe_id}.jpg"
                    filepath = os.path.join(DIRS["images"], filename)
                    
                    # Download high-res
                    r = requests.get(img_url, stream=True, timeout=30)
                    if r.status_code == 200:
                        with open(filepath, 'wb') as f:
                            for chunk in r.iter_content(1024):
                                f.write(chunk)
                        
                        data_buffer.append({
                            "id": item_id,
                            "source_id": SOURCE_ID,
                            "source_url": link,
                            "metadata": metadata,
                            "images": [{"file_name": filename, "original_url": img_url}]
                        })
            except Exception as e:
                print(f"Skipped {link}: {e}")
                
        await browser.close()
    return data_buffer

def save_and_upload(data):
    if not data: return
    
    # Save Metadata
    jsonl_path = os.path.join(DIRS["clean"], "data.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")
            
    # Move Images
    final_images_dir = os.path.join(DIRS["clean"], "images")
    os.makedirs(final_images_dir, exist_ok=True)
    for entry in data:
        for img in entry["images"]:
            shutil.copy2(os.path.join(DIRS["images"], img["file_name"]), 
                         os.path.join(final_images_dir, img["file_name"]))
    
    # Upload
    token = os.environ.get("HF_TOKEN")
    if token:
        print(f"☁️ Uploading to {REPO_ID}...")
        api = HfApi(token=token)
        api.create_repo(repo_id=REPO_ID, repo_type="dataset", exist_ok=True)
        api.upload_folder(folder_path=DIRS["clean"], repo_id=REPO_ID, repo_type="dataset", path_in_repo=".")
        print("🎉 Done!")

if __name__ == "__main__":
    setup_directories()
    data = asyncio.run(scrape_pitt_rivers())
    save_and_upload(data)

```
