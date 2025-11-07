import os
import json
import requests
import re
import time
import shutil
import logging
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import urljoin
from datetime import datetime
from PIL import Image, UnidentifiedImageError
from huggingface_hub import HfApi, create_repo
from copy import deepcopy

API_BASE_URL = "https://re-entanglements.net/wp-json/wp/v2/posts"
SITE_BASE_URL = "https://re-entanglements.net"
SOURCE_NAME = "Re-entanglements"
SOURCE_ID = "re-entanglements"
LOG_FILE = "re-entanglements_scraper.log"

IMAGE_REPO_ID = "nwokikeonyeka/re-entanglements-images"
AUDIO_REPO_ID = "nwokikeonyeka/re-entanglements-audio"

RAW_DIR = "data_re-entanglements_raw"
RAW_IMG_DIR = os.path.join(RAW_DIR, "images")
RAW_AUDIO_DIR = os.path.join(RAW_DIR, "audio")
RAW_JSONL = os.path.join(RAW_DIR, "data.jsonl")

CLEAN_IMAGES_DIR = "data_clean_images"
CLEAN_IMAGES_ASSETS = os.path.join(CLEAN_IMAGES_DIR, "images")
CLEAN_IMAGES_JSONL = os.path.join(CLEAN_IMAGES_DIR, "data.jsonl")
CLEAN_IMAGES_README = os.path.join(CLEAN_IMAGES_DIR, "README.md")

CLEAN_AUDIO_DIR = "data_clean_audio"
CLEAN_AUDIO_ASSETS = os.path.join(CLEAN_AUDIO_DIR, "audio")
CLEAN_AUDIO_JSONL = os.path.join(CLEAN_AUDIO_DIR, "data.jsonl")
CLEAN_AUDIO_README = os.path.join(CLEAN_AUDIO_DIR, "README.md")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

def get_json_response(url, params=None):
    try:
        headers = {'User-Agent': 'IgboArchives-ScraperBot/1.0'}
        r = requests.get(url, timeout=30, headers=headers, params=params)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from {url}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get JSON for {url} with params {params}: {e}")
        return None

def get_all_posts(api_url):
    logging.info("Finding all posts from the API...")
    all_posts = []
    page = 1
    PER_PAGE = 20 
    
    while True:
        logging.info(f"Querying API page {page} (found {len(all_posts)} posts so far)...")
        params = {
            'per_page': PER_PAGE, 
            'page': page,
            '_embed': 'wp:term'
        } 
        
        posts = get_json_response(api_url, params=params)
        
        if not posts or len(posts) == 0:
            if not posts:
                logging.error(f"API query failed at page {page}. Stopping.")
            else:
                logging.info(f"No more posts found at page {page}. This is the end.")
            break
            
        all_posts.extend(posts)
        page += 1
        time.sleep(1)
            
    logging.info(f"✅ Found {len(all_posts)} total posts across {page-1} pages.")
    return all_posts

def sanitize_filename(name):
    name = name.lower().replace(" ", "-")
    name = re.sub(r'[^\w\s.-]', '', name)
    name = re.sub(r'--+', '-', name)
    return name[:100]

def download_file(file_url, save_dir, post_id, index):
    try:
        r = requests.get(file_url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        data = r.content
        
        original_filename = os.path.basename(file_url.split("?")[0])
        safe_filename = sanitize_filename(original_filename)
        new_filename = f"{SOURCE_ID}_{post_id}_{index}_{int(time.time()*1000)}_{safe_filename}"
        save_path = os.path.join(save_dir, new_filename)
        
        with open(save_path, "wb") as f:
            f.write(data)
            
        file_size = os.path.getsize(save_path)
        file_stats = { "file_size_bytes": file_size, "width": None, "height": None }

        if save_dir == RAW_IMG_DIR:
            try:
                with Image.open(save_path) as img:
                    file_stats["width"], file_stats["height"] = img.size
            except UnidentifiedImageError:
                logging.warning(f"File {new_filename} is not a valid image. Skipping.")
                return None
        
        time.sleep(0.5)
        return (new_filename, file_stats)
        
    except Exception as e:
        logging.warning(f"Failed to download file {file_url}: {e}")
        return None

def process_post_json(post_json):
    post_id = post_json['id']
    post_url = post_json['link']
    title = BeautifulSoup(post_json.get('title', {}).get('rendered', ''), 'html.parser').get_text(strip=True)
    html_content = post_json.get('content', {}).get('rendered', '')
    soup = BeautifulSoup(html_content, 'html.parser')
    raw_text_content = soup.get_text("\n", strip=True)

    MODERN_IMAGE_KEYWORDS = r'(studio|workshop|exhibition|installation|artist|researcher|rehearsal|filming|screenshot|article|opening event|scenes from|Ozioma Onuzulike|RitaDoris|Kelani Abass|Chiadikōbi Nwaubani|Paul Basu|George Agbo|Chinyere Odinukwe|Chikaogwu Kanu|Ugonna Umeike|19[5-9]\d|20\d{2}|Stills from|interview|Chike Aniakor|Usifu Jalloh|Shakalearn Mansaray|conservation|Asogwa|Photomontage|project|treatment of|stages in|fieldwork)'
    MODERN_AUDIO_KEYWORDS = r'(discussing|interview|podcast|listen to|Paul Basu|Usifu Jalloh|Chijioke Onuora|Krydz Ikwuemesi|RitaDoris|Chinyere Odinukwe|Ngozi Omeje|Nicholas Thomas|BBC Radio|Ikenna Onwuegbuna|contemporary reworking|2019)'
    HISTORICAL_AUDIO_KEYWORDS = r'(NWT|BL C51|Northcote Thomas|cylinder|recording)'

    scraped_images = []
    for i, figure in enumerate(soup.select("figure:has(figcaption)")):
        img_tag = figure.select_one("img")
        if not img_tag: continue
        caption_tag = figure.select_one("figcaption")
        caption_text = caption_tag.get_text(strip=True) if caption_tag else ""
        img_url = img_tag.get('src')
        if not img_url: continue
        
        abs_img_url = urljoin(SITE_BASE_URL, img_url)
            
        if not re.search(MODERN_IMAGE_KEYWORDS, caption_text, re.IGNORECASE):
            img_stats = download_file(abs_img_url, RAW_IMG_DIR, post_id, i)
            if img_stats:
                new_filename, stats = img_stats
                scraped_images.append({
                    "file_name": new_filename, "original_url": abs_img_url,
                    "raw_caption": caption_text, "width": stats["width"],
                    "height": stats["height"], "file_size_bytes": stats["file_size_bytes"]
                })
        else:
            logging.info(f"Skipping modern-context image: '{caption_text[:50]}...'")

    scraped_audio = []
    for i, audio_tag in enumerate(soup.select("audio[src]")):
        audio_url = audio_tag.get('src')
        if not audio_url or not re.search(r'\.(mp3|ogg)(\?|$)', audio_url): continue
            
        caption = "Untitled Audio"
        parent_figure = audio_tag.find_parent("figure")
        if parent_figure:
            caption_tag = parent_figure.select_one("figcaption")
            if caption_tag: caption = caption_tag.get_text(strip=True)
            
        abs_audio_url = urljoin(SITE_BASE_URL, audio_url)

        is_explicitly_historical = re.search(HISTORICAL_AUDIO_KEYWORDS, caption, re.IGNORECASE)
        is_explicitly_modern = re.search(MODERN_AUDIO_KEYWORDS, caption, re.IGNORECASE)

        if (is_explicitly_historical or 'Untitled Audio' in caption) and not is_explicitly_modern:
            logging.info(f"Keeping historical/untitled audio: '{caption[:50]}...'")
            audio_stats = download_file(abs_audio_url, RAW_AUDIO_DIR, post_id, i)
            if audio_stats:
                new_filename, stats = audio_stats
                scraped_audio.append({
                    "file_name": new_filename, "original_url": abs_audio_url,
                    "raw_caption": caption, "file_size_bytes": stats["file_size_bytes"]
                })
        else:
            logging.info(f"Skipping modern/ambiguous audio: '{caption[:50]}...'")

    tags = []
    try:
        embedded_terms = post_json.get('_embedded', {}).get('wp:term', [])
        for term_list in embedded_terms:
            for term in term_list:
                if term.get('taxonomy') == 'post_tag':
                    tags.append(term.get('name'))
    except Exception as e:
        logging.warning(f"Could not parse embedded tags for post {post_id}: {e}")
    
    post_data = {
        "id": f"{SOURCE_ID}_{post_id}",
        "source_name": SOURCE_NAME,
        "source_type": "secondary",
        "original_url": post_url, 
        "title": title,
        "raw_content": raw_text_content,
        "images": scraped_images,
        "audio": scraped_audio,
        "tags_scraped": list(set(tags)),
        "license_info": "Copyright © 2025 [Re:]Entanglements",
        "timestamp_scraped": datetime.now().isoformat(),
        "source_specific_metadata": {
            "source_id": SOURCE_ID,
            "wp_post_id": post_id,
            "date_published": post_json.get('date')
        }
    }
    return post_data

def run_scraper():
    logging.info(f"--- [PART 1/4] Starting API scrape of {API_BASE_URL} ---")
    
    for path in [RAW_IMG_DIR, RAW_AUDIO_DIR]:
        os.makedirs(path, exist_ok=True)
        
    if os.path.exists(RAW_JSONL):
        logging.info(f"Removing old {RAW_JSONL}")
        os.remove(RAW_JSONL)
    
    all_posts_json = get_all_posts(API_BASE_URL)

    with open(RAW_JSONL, "w", encoding="utf-8") as f:
        for post in tqdm(all_posts_json, desc="Processing posts"):
            try:
                data = process_post_json(post)
                if data and (data['images'] or data['audio']):
                    f.write(json.dumps(data) + "\n")
            except Exception as e:
                logging.error(f"❌ Failed to process post ID {post.get('id')}: {e}")
    logging.info("✅ Scraper run complete.")

def run_cleaner_and_splitter():
    logging.info(f"\n--- [PART 2/4] Cleaning and Splitting the data ---")

    if os.path.exists(CLEAN_IMAGES_DIR): shutil.rmtree(CLEAN_IMAGES_DIR)
    if os.path.exists(CLEAN_AUDIO_DIR): shutil.rmtree(CLEAN_AUDIO_DIR)
    for path in [CLEAN_IMAGES_ASSETS, CLEAN_AUDIO_ASSETS]:
        os.makedirs(path, exist_ok=True)

    good_images = set()
    bad_images_count = 0
    image_files = os.listdir(RAW_IMG_DIR)
    audio_files = os.listdir(RAW_AUDIO_DIR)

    for filename in tqdm(image_files, desc="Validating images"):
        source_path = os.path.join(RAW_IMG_DIR, filename)
        clean_path = os.path.join(CLEAN_IMAGES_ASSETS, filename)
        try:
            with Image.open(source_path) as img:
                img.verify()
            shutil.copy(source_path, clean_path)
            good_images.add(filename)
        except Exception as e:
            bad_images_count += 1
            logging.warning(f"Skipping bad image {filename}: {e}")
    logging.info(f"Found and skipped {bad_images_count} bad images.")

    logging.info("Copying audio files to clean directory...")
    for filename in tqdm(audio_files, desc="Copying audio"):
        shutil.copy(os.path.join(RAW_AUDIO_DIR, filename), CLEAN_AUDIO_ASSETS)
    logging.info(f"Copied {len(audio_files)} audio files.")

    image_lines = 0
    audio_lines = 0
    if os.path.exists(RAW_JSONL):
        with open(RAW_JSONL, "r", encoding="utf-8") as f_in, \
             open(CLEAN_IMAGES_JSONL, "w", encoding="utf-8") as f_img_out, \
             open(CLEAN_AUDIO_JSONL, "w", encoding="utf-8") as f_aud_out:
            
            for line in f_in:
                data = json.loads(line)
                
                img_data = deepcopy(data) 
                aud_data = deepcopy(data) 
                
                img_data['images'] = [img for img in img_data['images'] if img['file_name'] in good_images]
                
                if img_data['images']:
                    del img_data['audio'] 
                    f_img_out.write(json.dumps(img_data) + "\n")
                    image_lines += 1
                    
                if aud_data['audio']:
                    del aud_data['images'] 
                    f_aud_out.write(json.dumps(aud_data) + "\n")
                    audio_lines += 1
    else:
        logging.error("raw data.jsonl file not found. Scraper may have failed.")

    logging.info(f"Wrote {image_lines} lines to {CLEAN_IMAGES_JSONL}.")
    logging.info(f"Wrote {audio_lines} lines to {CLEAN_AUDIO_JSONL}.")
    logging.info("✅ Cleaning and splitting complete.")
    
    return image_lines, len(good_images), audio_lines, len(audio_files)

def create_readmes(image_lines, image_count, audio_lines, audio_count):
    logging.info(f"\n--- [PART 3/4] Creating placeholder READMEs ---")
    readme_images = f"""---
dataset_info:
  license: other
---
# Re-entanglements (Images) Dataset
This dataset contains {image_lines} posts with {image_count} historical images scraped from the Re-entanglements project.
**This is a placeholder README.md. Full metadata will be added later.**
"""
    with open(CLEAN_IMAGES_README, "w", encoding="utf-8") as f:
        f.write(readme_images)

    readme_audio = f"""---
dataset_info:
  license: other
---
# Re-entanglements (Audio) Dataset
This dataset contains {audio_lines} posts with {audio_count} historical audio files (wax cylinder recordings) scraped from the Re-entanglements project.
**This is a placeholder README.md. Full metadata will be added later.**
"""
    with open(CLEAN_AUDIO_README, "w", encoding="utf-8") as f:
        f.write(readme_audio)
    logging.info("✅ Placeholder READMEs created.")

def upload_to_hf(token):
    logging.info(f"\n--- [PART 4/4] Uploading to Hugging Face ---")

    logging.info(f"Preparing to upload {CLEAN_IMAGES_DIR} to {IMAGE_REPO_ID}...")
    for attempt in range(3):
        try:
            api = HfApi(token=token)
            create_repo(IMAGE_REPO_ID, repo_type="dataset", token=token, exist_ok=True)
            api.upload_folder(
                folder_path=CLEAN_IMAGES_DIR,
                repo_id=IMAGE_REPO_ID,
                repo_type="dataset",
            )
            logging.info(f"✅ Successfully uploaded IMAGE dataset.")
            break
        except Exception as e:
            logging.error(f"\n❌ Image upload attempt {attempt + 1} failed: {e}")
            if attempt < 2: 
                logging.info("Retrying in 10 seconds...")
                time.sleep(10)
            else:
                logging.error("Final image upload attempt failed.")

    logging.info(f"Preparing to upload {CLEAN_AUDIO_DIR} to {AUDIO_REPO_ID}...")
    for attempt in range(3):
        try:
            api = HfApi(token=token)
            create_repo(AUDIO_REPO_ID, repo_type="dataset", token=token, exist_ok=True)
            api.upload_folder(
                folder_path=CLEAN_AUDIO_DIR,
                repo_id=AUDIO_REPO_ID,
                repo_type="dataset",
            )
            logging.info(f"✅ Successfully uploaded AUDIO dataset.")
            break
        except Exception as e:
            logging.error(f"\n❌ Audio upload attempt {attempt + 1} failed: {e}")
            if attempt < 2: 
                logging.info("Retrying in 10 seconds...")
                time.sleep(10)
            else:
                logging.error("Final audio upload attempt failed.")

def main():
    logging.info(f"--- Starting new {SOURCE_NAME} scrape (Production Version) ---")
    
    HF_TOKEN = os.getenv("HF_TOKEN")
    if not HF_TOKEN:
        HF_TOKEN = input("Paste your Hugging Face WRITE token: ").strip()
    
    if os.path.exists(RAW_DIR):
        logging.warning(f"Clearing old data from {RAW_DIR}...")
        shutil.rmtree(RAW_DIR)

    run_scraper()
    image_lines, image_count, audio_lines, audio_count = run_cleaner_and_splitter()
    create_readmes(image_lines, image_count, audio_lines, audio_count)
    upload_to_hf(HF_TOKEN)

    logging.info("\n" + "="*50)
    logging.info(f"✅✅✅ {SOURCE_NAME} SCRAPE COMPLETE! ✅✅✅")
    logging.info(f"Image dataset: https://huggingface.co/datasets/{IMAGE_REPO_ID}")
    logging.info(f"Audio dataset: https://huggingface.co/datasets/{AUDIO_REPO_ID}")
    logging.info("="*50)

if __name__ == "__main__":
    main()
