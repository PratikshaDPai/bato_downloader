import os
import time
import json
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from bato_scraper import get_manga_info, download_chapter, sanitize_filename

# --- CONFIG ---
INPUT_FILE = "series_list.txt"   # one series URL per line
OUTPUT_DIR = "output"            # root output folder
MAX_WORKERS = 10                 # threads for images per chapter
RETRY_DELAY = 10                 # seconds before retry
FAILED_LOG = "failed_chapters.json"
# ---------------

# Queue for finished chapters (to be converted)
chapter_queue = queue.Queue()
stop_event = threading.Event()


def load_failed_chapters():
    if os.path.exists(FAILED_LOG):
        with open(FAILED_LOG, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []


def save_failed_chapters(failed):
    if not failed:
        return
    with open(FAILED_LOG, "w", encoding="utf-8") as f:
        json.dump(failed, f, indent=2, ensure_ascii=False)


def read_series_urls():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def download_series(series_url):
    print("=" * 80)
    print(f"Fetching manga info: {series_url}")
    print("=" * 80)

    try:
        manga_title, chapters = get_manga_info(series_url)
    except Exception as e:
        print(f"Error fetching series info: {e}")
        return []

    if not chapters:
        print("No chapters found.")
        return []

    manga_sanitized = sanitize_filename(manga_title)
    series_dir = os.path.join(OUTPUT_DIR, manga_sanitized)
    os.makedirs(series_dir, exist_ok=True)

    failed = []
    total = len(chapters)
    pad_length = len(str(total))

    for index, chapter in enumerate(chapters,start=1):
        prefix = str(index).zfill(pad_length)
        clean_title = sanitize_filename(chapter["title"])
        numbered_title = f"{prefix}_{clean_title}"

        print(f"\n[{index + 1}/{total}] Downloading: {numbered_title}")

        try:
            download_chapter(
                chapter_url=chapter["url"],
                manga_title=manga_title,
                chapter_title=numbered_title,
                output_dir=OUTPUT_DIR,
                stop_event=threading.Event(),
                convert_to_pdf=False,
                convert_to_cbz=False,   # let queue handle conversion
                keep_images=True,       # converter needs images
                max_workers=MAX_WORKERS,
            )
            chapter_dir = os.path.join(
                OUTPUT_DIR,
                sanitize_filename(manga_title),
                sanitize_filename(numbered_title),
            )
            chapter_queue.put(
                {
                    "chapter_dir": chapter_dir,
                    "manga_title": manga_title,
                    "chapter_title": numbered_title,
                }
            )
        except Exception as e:
            print(f"Failed to download {numbered_title}: {e}")
            failed.append(
                {
                    "manga_title": manga_title,
                    "chapter_title": numbered_title,
                    "chapter_url": chapter["url"],
                }
            )
            save_failed_chapters(failed)
            continue

    return failed


def retry_failed():
    failed = load_failed_chapters()
    if not failed:
        print("No failed chapters to retry.")
        return

    print(f"\nRetrying {len(failed)} failed chapters...")
    new_failed = []

    for chap in failed:
        try:
            download_chapter(
                chapter_url=chap["chapter_url"],
                manga_title=chap["manga_title"],
                chapter_title=chap["chapter_title"],
                output_dir=OUTPUT_DIR,
                stop_event=threading.Event(),
                convert_to_pdf=False,
                convert_to_cbz=False,  # queue will convert
                keep_images=True,
                max_workers=MAX_WORKERS,
            )
            chapter_dir = os.path.join(
                OUTPUT_DIR,
                sanitize_filename(chap["manga_title"]),
                sanitize_filename(chap["chapter_title"]),
            )
            chapter_queue.put(
                {
                    "chapter_dir": chapter_dir,
                    "manga_title": chap["manga_title"],
                    "chapter_title": chap["chapter_title"],
                }
            )
        except Exception as e:
            print(f"Retry failed for {chap['chapter_title']}: {e}")
            new_failed.append(chap)
            continue

    save_failed_chapters(new_failed)
    if new_failed:
        print(
            f"\nStill failed after retry: {len(new_failed)} chapters. Stored in {FAILED_LOG}."
        )
    else:
        print("\nAll previously failed chapters retried successfully!")
        if os.path.exists(FAILED_LOG):
            os.remove(FAILED_LOG)


def converter_worker():
    """Continuously watches the chapter_queue and converts chapters to CBZ."""
    from bato_scraper import convert_chapter_to_cbz

    while not stop_event.is_set() or not chapter_queue.empty():
        try:
            item = chapter_queue.get(timeout=2)
        except queue.Empty:
            continue

        chapter_dir = item["chapter_dir"]
        manga_title = item["manga_title"]
        chapter_title = item["chapter_title"]

        try:
            print(f"Converting to CBZ: {chapter_title}")
            convert_chapter_to_cbz(
                chapter_dir, manga_title, chapter_title, delete_images=True
            )
            print(f"Converted: {chapter_title}")
        except Exception as e:
            print(f"Conversion failed for {chapter_title}: {e}")
        finally:
            chapter_queue.task_done()


# -------------------------------------------------------------------------
if __name__ == "__main__":
    series_urls = read_series_urls()
    all_failed = []

    # Start converter background thread
    converter_thread = threading.Thread(target=converter_worker, daemon=True)
    converter_thread.start()

    SERIES_WORKERS = 2  # how many series at once
    with ThreadPoolExecutor(max_workers=SERIES_WORKERS) as executor:
        futures = [executor.submit(download_series, url) for url in series_urls]
        for future in futures:
            failed = future.result()
            if failed:
                print(
                    f"\n{len(failed)} failed chapters in this series logged for retry."
                )
                all_failed.extend(failed)
                save_failed_chapters(all_failed)
                print(f"Waiting {RETRY_DELAY} seconds before continuing...")
                time.sleep(RETRY_DELAY)

    retry_failed()          # retry queue-fed failures
    chapter_queue.join()    # wait for conversions
    stop_event.set()
    converter_thread.join()

    print("\nAll downloads and CBZ conversions complete!")
