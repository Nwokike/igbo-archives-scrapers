import os
import asyncio
import json
import shutil
import requests
from playwright.async_api import async_playwright
from huggingface_hub import HfApi, create_repo
from tqdm import tqdm
from PIL import Image as PILImage

# --- CONFIGURATION ---
REPO_ID = "nwokikeonyeka/maa-cambridge-south-eastern-nigeria"
SEARCH_URL = "https://collections.maa.cam.ac.uk/photographs/?advanced_search=%5B%7B%22field%22%3A%22place%22%2C%22value%22%3A%22South+Eastern+Nigeria%22%7D%5D&filters=image_available"

# Standardizing output folders to match your other scrapers
BASE_DIR = "data_maa"
DIRS = {
    "raw": os.path.join(BASE_DIR, "raw"),
    "images": os.path.join(BASE_DIR, "images"),
    "clean": os.path.join(BASE_DIR, "clean")
}

def setup_directories():
    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

async def scrape_maa():
    data_buffer = []
    
    async with async_playwright() as p:
        print("🚀 Launching Browser...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        print(f"🌍 Accessing Search: {SEARCH_URL}")
        try:
            await page.goto(SEARCH_URL, timeout=60000)
            
            # Remove Cookie Banner
            await page.evaluate(r"""() => {
                const banners = document.querySelectorAll('div[class*="cookie"], #cookie-banner');
                banners.forEach(b => b.remove());
            }""")
            
            print("⏳ Waiting for results...")
            await page.wait_for_selector("text=Search returned", timeout=60000)

        except Exception as e:
            print(f"❌ Error loading search: {e}")
            await browser.close()
            return []

        # --- A. HARVEST LINKS ---
        object_links = set()
        page_num = 1
        
        while True:
            print(f"   Scanning Page {page_num}...", end="\r")
            
            try:
                # Extract Item Links
                hrefs = await page.evaluate(r'''() => {
                    return Array.from(document.querySelectorAll('a')).map(a => a.href)
                }''')
                
                # Filter for Item URLs
                new_links = []
                for h in hrefs:
                    if "/photographs/" in h and any(c.isdigit() for c in h.split("/")[-1]):
                        if "page=" not in h and "filters=" not in h:
                            new_links.append(h.split("?")[0])

                unique_new = [l for l in set(new_links) if l not in object_links]
                object_links.update(unique_new)
                
                if not unique_new and page_num > 1:
                    print(f"\n✅ Finished scanning. Total pages: {page_num}")
                    break

                # Next Page Logic
                page_num += 1
                next_url = f"{SEARCH_URL}&page={page_num}"
                
                response = await page.goto(next_url, timeout=60000)
                if not response.ok: break
                
                try:
                    await page.wait_for_selector("text=Search returned", state="visible", timeout=30000)
                except:
                    print(f"\n⚠️ End of results at page {page_num}.")
                    break
                    
            except Exception as e:
                print(f"Loop error: {e}")
                break

        print(f"\n🔗 Total objects found: {len(object_links)}")
        
        # --- B. SCRAPE DETAILS ---
        if object_links:
            print("📸 Scraping objects...")
            
            link_list = list(object_links)
            
            for i, link in enumerate(tqdm(link_list)):
                try:
                    await page.goto(link, timeout=45000)
                    
                    # 1. Scrape Metadata
                    metadata = await page.evaluate(r'''() => {
                        let data = {};
                        let h1 = document.querySelector('h1');
                        data.title = h1 ? h1.innerText.trim() : "Untitled";
                        
                        document.querySelectorAll('.d-flex.flex-wrap').forEach(row => {
                            let keyEl = row.querySelector('.fw-bold');
                            let valEl = row.querySelector('.col-12.col-md-8');
                            if(keyEl && valEl) {
                                let cleanKey = keyEl.innerText.replace(':','').trim().replace(/ /g, '_').toLowerCase();
                                data[cleanKey] = valEl.innerText.trim();
                            }
                        });
                        return data;
                    }''')
                    
                    # 2. Scrape Images
                    img_urls = await page.evaluate(r'''() => {
                        let srcs = new Set();
                        const container = document.querySelector('#images');
                        if (container) {
                            container.querySelectorAll('a').forEach(a => {
                                if (a.href && a.href.match(/\.(png|jpg|jpeg|webp)$/i)) srcs.add(a.href);
                            });
                            if (srcs.size === 0) {
                                container.querySelectorAll('img').forEach(img => srcs.add(img.src));
                            }
                        }
                        return Array.from(srcs);
                    }''')
                    
                    saved_imgs = []
                    idno = metadata.get("idno", f"maa_unknown_{i}")
                    
                    for idx, u in enumerate(img_urls):
                        if "media/" not in u and "collections.maa" not in u: continue
                        
                        try:
                            r = requests.get(u, timeout=15)
                            if r.status_code == 200:
                                safe_id = idno.replace(".", "_").replace(" ", "_").replace("/", "-").strip()
                                fname = f"{safe_id}_{idx}.jpg"
                                fpath = os.path.join(DIRS["images"], fname)
                                with open(fpath, "wb") as f: f.write(r.content)
                                saved_imgs.append({"file_name": fname, "original_url": u})
                        except: pass
                    
                    if metadata:
                        data_buffer.append({
                            "id": idno,
                            "source_url": link,
                            "metadata": metadata,
                            "images": saved_imgs,
                            "source_id": "maa_cambridge"
                        })

                except Exception as e:
                    print(f"Skipped {link}: {e}")
                    continue

        await browser.close()
    return data_buffer

def save_and_repack(data):
    print("📦 Processing and Repacking...")
    
    # Save Raw
    raw_path = os.path.join(DIRS["raw"], "data.jsonl")
    with open(raw_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
            
    # Clean & Move to Final Folder (Images inside /clean/images/)
    final_clean_dir = DIRS["clean"]
    final_images_dir = os.path.join(final_clean_dir, "images")
    os.makedirs(final_images_dir, exist_ok=True)
    final_jsonl = os.path.join(final_clean_dir, "data.jsonl")
    
    valid_count = 0
    with open(final_jsonl, "w", encoding="utf-8") as f_out:
        for item in tqdm(data, desc="Validating"):
            valid_images = []
            for img in item.get("images", []):
                src = os.path.join(DIRS["images"], img["file_name"])
                dst = os.path.join(final_images_dir, img["file_name"])
                
                try:
                    if os.path.exists(src):
                        with PILImage.open(src) as pi: pi.verify()
                        shutil.copy2(src, dst)
                        valid_images.append(img)
                except: pass
            
            if valid_images:
                item["images"] = valid_images
                f_out.write(json.dumps(item) + "\n")
                valid_count += 1
                
    print(f"✅ Ready: {valid_count} items in {final_clean_dir}")
    return valid_count

def upload_to_hf():
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("⚠️ HF_TOKEN not found. Skipping upload.")
        return

    print(f"☁️ Uploading to {REPO_ID}...")
    api = HfApi(token=token)
    api.create_repo(repo_id=REPO_ID, repo_type="dataset", exist_ok=True)
    
    try:
        api.upload_large_folder(
            folder_path=DIRS["clean"],
            repo_id=REPO_ID,
            repo_type="dataset",
            path_in_repo="."
        )
        print("🎉 Upload Success!")
    except Exception as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    setup_directories()
    data = asyncio.run(scrape_maa())
    if data:
        save_and_repack(data)
        # upload_to_hf() # Uncomment to auto-upload
