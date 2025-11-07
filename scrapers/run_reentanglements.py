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

AUDIO_REPO_ID = "nwokikeonyeka/re-entanglements-audio"
DOCUMENT_REPO_ID = "nwokikeonyeka/re-entanglements-documents"

RAW_DIR = "data_re-entanglements_raw"
RAW_AUDIO_DIR = os.path.join(RAW_DIR, "audio")
RAW_DOC_DIR = os.path.join(RAW_DIR, "documents") 
RAW_JSONL = os.path.join(RAW_DIR, "data.jsonl")

CLEAN_AUDIO_DIR = "data_clean_audio"
CLEAN_AUDIO_ASSETS = os.path.join(CLEAN_AUDIO_DIR, "audio")
CLEAN_AUDIO_JSONL = os.path.join(CLEAN_AUDIO_DIR, "data.jsonl")
CLEAN_AUDIO_README = os.path.join(CLEAN_AUDIO_DIR, "README.md")

CLEAN_DOCUMENTS_DIR = "data_clean_documents"
CLEAN_DOCUMENTS_ASSETS = os.path.join(CLEAN_DOCUMENTS_DIR, "documents")
CLEAN_DOCUMENTS_JSONL = os.path.join(CLEAN_DOCUMENTS_DIR, "data.jsonl")
CLEAN_DOCUMENTS_README = os.path.join(CLEAN_DOCUMENTS_DIR, "README.md")

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
        logging.warning(f"Failed to get JSON for {url} with params {params}: {e}")
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
                logging.warning(f"API query failed at page {page}. (This is expected at the end of pagination).")
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

        if save_dir == RAW_DOC_DIR:
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

    MODERN_IMAGE_KEYWORDS = r'(studio|workshop|exhibition|installation|artist|researcher|rehearsal|filming|screenshot|article|opening event|scenes from|Ozioma Onuzulike|RitaDoris|Kelani Abass|Chiadikōbi Nwaubani|Paul Basu|George Agbo|Chinyere Odinukwe|Chikaogwu Kanu|Ugonna Umeike|19[5-9]\d|20\d{2}|Stills from|interview|Chike Aniakor|Usifu Jalloh|Shakalearn Mansaray|conservation|Asogwa|Photomontage|project|treatment of|stages in|fieldwork|M. V. Portman|Edison phonograph|selection of instruments|recent colour photograph|team members|in the lab|Art Assassins|Onyeka Igwe|Dr Janet Topp Fargion|Felix Ekhator|Raphael Anaemena|Hassan Jalloh|Presentations from|Katrina Dring|Works-in-progress)'
    DOCUMENT_KEYWORDS = r'(Notes and Queries|Statistical analysis|Page proofs|Letter from|Annual Report|herbarium specimens|catalogue|Kew Bulletin|pages from|excerpt from|edition of|album|labels|label|Appendix C|document|manuscript|transcription|sketch map)'
    MODERN_AUDIO_KEYWORDS = r'(discussing|interview|podcast|listen to|Paul Basu|Usifu Jalloh|Chijioke Onuora|Krydz Ikwuemesi|RitaDoris|Chinyere Odinukwe|Ngozi Omeje|Nicholas Thomas|BBC Radio|Ikenna Onwuegbuna|contemporary reworking|2019)'
    HISTORICAL_AUDIO_KEYWORDS = r'(NWT|BL C51|Northcote Thomas|cylinder|recording)'

    scraped_documents = []
    
    for i, figure in enumerate(soup.select("figure:has(figcaption)")):
        img_tag = figure.select_one("img")
        if not img_tag: continue
        
        caption_tag = figure.select_one("figcaption")
        caption_text = caption_tag.get_text(strip=True) if caption_tag else ""
        img_url = img_tag.get('src')
        if not img_url: continue
        
        abs_img_url = urljoin(SITE_BASE_URL, img_url)
        
        if re.search(MODERN_IMAGE_KEYWORDS, caption_text, re.IGNORECASE):
            logging.info(f"Skipping MODERN image: '{caption_text[:50]}...'")
            continue 
        
        if re.search(DOCUMENT_KEYWORDS, caption_text, re.IGNORECASE):
            logging.info(f"Sorting as DOCUMENT: '{caption_text[:50]}...'")
            img_stats = download_file(abs_img_url, RAW_DOC_DIR, post_id, i)
            if img_stats:
                new_filename, stats = img_stats
                scraped_documents.append({
                    "file_name": new_filename, "original_url": abs_img_url,
                    "raw_caption": caption_text, "width": stats["width"],
                    "height": stats["height"], "file_size_bytes": stats["file_size_bytes"]
                })
        else:
            logging.info(f"Skipping (non-document) image: '{caption_text[:50]}...'")
            pass

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
        is_historical = re.search(HISTORICAL_AUDIO_KEYWORDS, caption, re.IGNORECASE)
        is_modern = re.search(MODERN_AUDIO_KEYWORDS, caption, re.IGNORECASE)

        if (is_historical or 'Untitled Audio' in caption) and not is_modern:
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
        "audio": scraped_audio,
        "documents": scraped_documents,
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
    
    for path in [RAW_AUDIO_DIR, RAW_DOC_DIR]:
        os.makedirs(path, exist_ok=True)
        
    if os.path.exists(RAW_JSONL):
        logging.info(f"Removing old {RAW_JSONL}")
        os.remove(RAW_JSONL)
    
    all_posts_json = get_all_posts(API_BASE_URL)

    with open(RAW_JSONL, "w", encoding="utf-8") as f:
        for post in tqdm(all_posts_json, desc="Processing posts"):
            try:
                data = process_post_json(post)
                if data and (data['audio'] or data['documents']):
                    f.write(json.dumps(data) + "\n")
            except Exception as e:
                logging.error(f"❌ Failed to process post ID {post.get('id')}: {e}")
    logging.info("✅ Scraper run complete.")

def run_cleaner_and_splitter():
    logging.info(f"\n--- [PART 2/4] Cleaning and Splitting the data ---")

    if os.path.exists(CLEAN_AUDIO_DIR): shutil.rmtree(CLEAN_AUDIO_DIR)
    if os.path.exists(CLEAN_DOCUMENTS_DIR): shutil.rmtree(CLEAN_DOCUMENTS_DIR)
    for path in [CLEAN_AUDIO_ASSETS, CLEAN_DOCUMENTS_ASSETS]:
        os.makedirs(path, exist_ok=True)

    good_documents = set()
    bad_documents_count = 0
    
    if os.path.exists(RAW_DOC_DIR):
        doc_files = os.listdir(RAW_DOC_DIR)
        for filename in tqdm(doc_files, desc="Validating Documents"):
            source_path = os.path.join(RAW_DOC_DIR, filename)
            clean_path = os.path.join(CLEAN_DOCUMENTS_ASSETS, filename)
            try:
                with Image.open(source_path) as img:
                    img.verify()
                shutil.copy(source_path, clean_path)
                good_documents.add(filename)
            except Exception as e:
                bad_documents_count += 1
                logging.warning(f"Skipping bad document image {filename}: {e}")
        logging.info(f"Validated and moved {len(good_documents)} documents. Skipped {bad_documents_count} bad documents.")
    else:
        logging.info("No raw document directory found. Skipping validation.")

    audio_files = []
    if os.path.exists(RAW_AUDIO_DIR):
        audio_files = os.listdir(RAW_AUDIO_DIR)
        logging.info("Copying audio files...")
        for filename in tqdm(audio_files, desc="Copying Audio"):
            shutil.copy(os.path.join(RAW_AUDIO_DIR, filename), CLEAN_AUDIO_ASSETS)
        logging.info(f"Copied {len(audio_files)} audio files.")
    else:
        logging.info("No raw audio directory found. Skipping copy.")

    audio_lines = 0
    document_lines = 0

    if os.path.exists(RAW_JSONL):
        with open(RAW_JSONL, "r", encoding="utf-8") as f_in, \
             open(CLEAN_AUDIO_JSONL, "w", encoding="utf-8") as f_aud_out, \
             open(CLEAN_DOCUMENTS_JSONL, "w", encoding="utf-8") as f_doc_out:
            
            for line in f_in:
                data = json.loads(line)
                
                if data.get('audio'):
                    aud_data = deepcopy(data)
                    if aud_data['audio']:
                        if 'documents' in aud_data: del aud_data['documents']
                        f_aud_out.write(json.dumps(aud_data) + "\n")
                        audio_lines += 1

                if data.get('documents'):
                    doc_data = deepcopy(data)
                    doc_data['documents'] = [doc for doc in doc_data['documents'] if doc['file_name'] in good_documents]
                    if doc_data['documents']:
                        if 'audio' in doc_data: del doc_data['audio']
                        f_doc_out.write(json.dumps(doc_data) + "\n")
                        document_lines += 1
    else:
        logging.error("raw data.jsonl file not found. Scraper may have failed.")

    logging.info(f"Wrote {audio_lines} lines to {CLEAN_AUDIO_JSONL}.")
    logging.info(f"Wrote {document_lines} lines to {CLEAN_DOCUMENTS_JSONL}.")
    logging.info("✅ Cleaning and splitting complete.")
    
    return audio_lines, len(audio_files), document_lines, len(good_documents)

def create_readmes(audio_lines, audio_count, document_lines, document_count):
    logging.info(f"\n--- [PART 3/4] Creating placeholder READMEs ---")

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

    readme_documents = f"""---
dataset_info:
  license: other
---
# Re-entanglements (Documents) Dataset
This dataset contains {document_lines} posts with {document_count} images of historical documents (letters, catalogue pages, specimens, charts) scraped from the Re-entanglements project.
**This is a placeholder README.md. Full metadata will be added later.**
"""
    with open(CLEAN_DOCUMENTS_README, "w", encoding="utf-8") as f:
        f.write(readme_documents)
    logging.info("✅ 2 Placeholder READMEs created.")

def upload_to_hf(token):
    logging.info(f"\n--- [PART 4/4] Uploading to Hugging Face ---")
    api = HfApi(token=token)

    logging.info(f"Preparing to upload {CLEAN_AUDIO_DIR} to {AUDIO_REPO_ID}...")
    for attempt in range(3):
        try:
            create_repo(AUDIO_REPO_ID, repo_type="dataset", token=token, exist_ok=True)
            api.upload_folder(
                folder_path=CLEAN_AUDIO_DIR, repo_id=AUDIO_REPO_ID, repo_type="dataset"
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

    logging.info(f"Preparing to upload {CLEAN_DOCUMENTS_DIR} to {DOCUMENT_REPO_ID}...")
    for attempt in range(3):
        try:
            create_repo(DOCUMENT_REPO_ID, repo_type="dataset", token=token, exist_ok=True)
            api.upload_folder(
                folder_path=CLEAN_DOCUMENTS_DIR, repo_id=DOCUMENT_REPO_ID, repo_type="dataset"
            )
            logging.info(f"✅ Successfully uploaded DOCUMENT dataset.")
            break
        except Exception as e:
            logging.error(f"\n❌ Document upload attempt {attempt + 1} failed: {e}")
            if attempt < 2: 
                logging.info("Retrying in 10 seconds...")
                time.sleep(10)
            else:
                logging.error("Final document upload attempt failed.")

def main():
    logging.info(f"--- Starting new {SOURCE_NAME} scrape (Audio/Docs Only, Correct Logic) ---")
    
    HF_TOKEN = os.getenv("HF_TOKEN")
    if not HF_TOKEN:
        HF_TOKEN = input("Paste your Hugging Face WRITE token: ").strip()
    
    if os.path.exists(RAW_DIR):
        logging.warning(f"Clearing old data from {RAW_DIR}...")
        shutil.rmtree(RAW_DIR)

    run_scraper()
    audio_lines, audio_count, document_lines, document_count = run_cleaner_and_splitter()
    create_readmes(audio_lines, audio_count, document_lines, document_count)
    upload_to_hf(HF_TOKEN)

    logging.info("\n" + "="*50)
    logging.info(f"✅✅✅ {SOURCE_NAME} SCRAPE COMPLETE! ✅✅✅")
    logging.info(f"Audio dataset: https://huggingface.co/datasets/{AUDIO_REPO_ID}")
    logging.info(f"Document dataset: https://huggingface.co/datasets/{DOCUMENT_REPO_ID}")
    logging.info("="*50)

if __name__ == "__main__":
    main()
